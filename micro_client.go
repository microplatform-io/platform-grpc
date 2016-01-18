package platform_grpc

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/microplatform-io/platform"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var logger = log.New(os.Stdout, "[platform-grpc] ", log.Ldate|log.Ltime)

type MicroClientConfig struct {
	EndpointGetter func() string

	// TLS details
	CertFile string
	Domain   string
}

type MicroClient struct {
	config                 MicroClientConfig
	clientConn             *grpc.ClientConn
	client                 RouterClient
	stream                 Router_RouteClient
	pendingResponses       map[string]chan *platform.Request
	transportAuthenticator credentials.TransportAuthenticator

	mu *sync.Mutex
}

func (mc *MicroClient) Close() error {
	mc.stream.CloseSend()

	return mc.clientConn.Close()
}

func (mc *MicroClient) Reconnect() error {
	mc.stream.CloseSend()
	mc.clientConn.Close()

	return mc.rebuildStream()
}

func (mc *MicroClient) createPendingResponseChan(request *platform.Request) chan *platform.Request {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.pendingResponses[request.GetUuid()] = make(chan *platform.Request, 5)

	return mc.pendingResponses[request.GetUuid()]
}

func (mc *MicroClient) getPendingResponseChan(request *platform.Request) (chan *platform.Request, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if pendingResponseChan, exists := mc.pendingResponses[request.GetUuid()]; exists {
		return pendingResponseChan, nil
	} else {
		return nil, errors.New("pending response chan not found")
	}
}

func (mc *MicroClient) deletePendingResponseChan(request *platform.Request) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, exists := mc.pendingResponses[request.GetUuid()]; exists {
		delete(mc.pendingResponses, request.GetUuid())
	}
}

func (mc *MicroClient) rebuildStream() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("failed to rebuild stream: %s", r))
		}
	}()

	clientConn, err := grpc.Dial(mc.config.EndpointGetter(), grpc.WithTransportCredentials(mc.transportAuthenticator))
	if err != nil {
		return err
	}

	mc.clientConn = clientConn
	mc.client = NewRouterClient(clientConn)

	stream, err := mc.client.Route(context.Background())
	if err != nil {
		return err
	}

	mc.stream = stream

	go func() {
		for {
			grpcResponse, err := stream.Recv()
			if err != nil {
				logger.Printf("[MicroClient.Route] failed to recv client response: %s", err)
				return
			}

			response := &platform.Request{}
			if err := platform.Unmarshal(grpcResponse.Payload, response); err != nil {
				logger.Printf("[MicroClient.Route] failed to unmarshal platform response: %s", err)
				continue
			}

			if pendingResponseChan, err := mc.getPendingResponseChan(response); err == nil {
				select {
				case pendingResponseChan <- response:
					logger.Printf("[MicroClient.Route] %s - successfully routed response to caller", response.GetUuid())
				case <-time.After(time.Millisecond * 50):
					logger.Printf("[MicroClient.Route] %s - failed to send to callback due to a blocked channel", response.GetUuid())
				}
			} else {
				logger.Printf("[MicroClient.Route] %s - failed to find pending response chan: %s", response.GetUuid(), err)
			}
		}
	}()

	return nil
}

func (mc *MicroClient) Route(request *platform.Request) (chan *platform.Request, chan interface{}) {
	request.Uuid = platform.String(platform.CreateUUID())

	clientResponses := make(chan *platform.Request, 5)
	streamTimeout := make(chan interface{})

	logger.Printf("[MicroClient.Route] %s - creating pending responses chan", request.GetUuid())

	internalResponses := mc.createPendingResponseChan(request)

	payload, _ := proto.Marshal(request)

	logger.Printf("[MicroClient.Route] %s - sending platform request", request.GetUuid())

	var sendError error

	for i := 0; i < 3; i++ {
		sendError = mc.stream.Send(&Request{
			Payload: payload,
		})

		if sendError != nil {
			logger.Printf("[MicroClient.Route] Error on sending grpc request, retrying: %s", sendError)

			if err := mc.rebuildStream(); err != nil {
				panic("[MicroClient.Route] A new connection could not be established, Panicking!")
			}
		} else {
			break
		}
	}

	if sendError != nil {
		panic("[MicroClient.Route] Failed to send the grpc request multiple times")
	}

	ready := make(chan interface{})

	go func() {
		timer := time.NewTimer(7 * time.Second)
		defer timer.Stop()
		defer mc.deletePendingResponseChan(request)

		ready <- true

		for {
			select {
			case response := <-internalResponses:
				timer.Reset(7 * time.Second)

				logger.Printf("[MicroClient.Route] %s - got response: %s", request.GetUuid(), response.Routing.RouteTo[0].GetUri())

				if response.Routing.RouteTo[0].GetUri() == "resource:///heartbeat" {
					continue
				}

				select {
				case clientResponses <- response:
					logger.Printf("[MicroClient.Route] %s - reply chan was available", request.GetUuid())

				case <-time.After(250 * time.Millisecond):
					logger.Printf("[MicroClient.Route] %s - reply chan was not available", request.GetUuid())
				}

				if response.GetCompleted() {
					logger.Printf("[MicroClient.Route] %s - final response, ending goroutine", request.GetUuid())
					return
				}

			case <-timer.C:
				logger.Printf("[MicroClient.Route] %s - stream has timed out", request.GetUuid())

				select {
				case streamTimeout <- nil:
					logger.Printf("[MicroClient.Route] %s - notified client of stream timeout", request.GetUuid())
				default:
					logger.Printf("[MicroClient.Route] %s - failed to notify client of stream timeout", request.GetUuid())
				}

				return
			}
		}
	}()

	logger.Printf("[MicroClient.Route] %s - waiting for responses goroutine", request.GetUuid())

	<-ready

	return clientResponses, streamTimeout
}

func NewMicroClient(config MicroClientConfig) (*MicroClient, error) {
	transportAuthenticator, err := credentials.NewClientTLSFromFile(config.CertFile, config.Domain)
	if err != nil {
		return nil, err
	}

	microClient := &MicroClient{
		config:                 config,
		transportAuthenticator: transportAuthenticator,

		pendingResponses: make(map[string]chan *platform.Request),
		mu:               &sync.Mutex{},
	}

	if err := microClient.rebuildStream(); err != nil {
		return nil, err
	}

	return microClient, nil
}
