package platform_grpc

import (
	"github.com/golang/protobuf/proto"
	"github.com/microplatform-io/platform"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"os"
	"sync"
	"time"
)

var logger = log.New(os.Stdout, "[platform-grpc] ", log.Ldate|log.Ltime)

type MicroClientConfig struct {
	Endpoint string

	// TLS details
	CertFile string
	Domain   string

	SupportHeartbeats bool
}

type MicroClient struct {
	config           MicroClientConfig
	clientConn       *grpc.ClientConn
	client           RouterClient
	stream           Router_RouteClient
	pendingResponses map[string]chan *platform.Request
	mu               *sync.Mutex
}

func (mc *MicroClient) Close() error {
	mc.stream.CloseSend()

	return mc.clientConn.Close()
}

func (mc *MicroClient) rebuildStream() error {
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

			platformResponse := &platform.Request{}
			if err := platform.Unmarshal(grpcResponse.Payload, platformResponse); err != nil {
				logger.Printf("[MicroClient.Route] failed to unmarshal platform response: %s", err)
				continue
			}

			mc.mu.Lock()
			pendingResponseChan, exists := mc.pendingResponses[platformResponse.GetUuid()]
			mc.mu.Unlock()

			if exists {
				select {
				case pendingResponseChan <- platformResponse:
					logger.Printf("[MicroClient.Route] %s - successfully routed response to caller", platformResponse.GetUuid())
				default:
					logger.Printf("[MicroClient.Route] %s - failed to send to callback due to a blocked channel", platformResponse.GetUuid())
				}
			} else {
				logger.Printf("[MicroClient.Route] %s - got a response for an unknown request uuid: %s", platformResponse.GetUuid(), platformResponse)
			}
		}
	}()

	return nil
}

func (mc *MicroClient) Route(request *platform.Request) (chan *platform.Request, chan interface{}) {
	request.Uuid = platform.String(platform.CreateUUID())

	internalResponses := make(chan *platform.Request)
	clientResponses := make(chan *platform.Request)
	streamTimeout := make(chan interface{})
	ready := make(chan interface{})

	go func() {
		ready <- true

		for {
			heartbeatTimeout := make(<-chan time.Time)
			if mc.config.SupportHeartbeats {
				heartbeatTimeout = time.After(1000 * time.Millisecond)
			}

			select {
			case response := <-internalResponses:
				logger.Printf("[MicroClient.Route] %s - got response: %s", request.GetUuid(), response.Routing.RouteTo[0].GetUri())

				if response.Routing.RouteTo[0].GetUri() == "resource:///heartbeat" {
					continue
				}

				clientResponses <- response

				if response.GetCompleted() {
					logger.Printf("[MicroClient.Route] %s - final response, ending goroutine", request.GetUuid())
					return
				}

			case <-heartbeatTimeout:
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

	logger.Printf("[MicroClient.Route] %s - assigning pending responses chan to internal responses", request.GetUuid())

	mc.mu.Lock()
	mc.pendingResponses[request.GetUuid()] = internalResponses
	mc.mu.Unlock()

	payload, _ := proto.Marshal(request)

	logger.Printf("[MicroClient.Route] %s - sending platform request", request.GetUuid())

	mc.stream.Send(&Request{
		Payload: payload,
	})

	return clientResponses, streamTimeout
}

func NewMicroClient(config MicroClientConfig) (*MicroClient, error) {
	creds, err := credentials.NewClientTLSFromFile(config.CertFile, config.Domain)
	if err != nil {
		return nil, err
	}

	clientConn, err := grpc.Dial(config.Endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}

	microClient := &MicroClient{
		config:           config,
		clientConn:       clientConn,
		client:           NewRouterClient(clientConn),
		pendingResponses: make(map[string]chan *platform.Request),
		mu:               &sync.Mutex{},
	}

	if err := microClient.rebuildStream(); err != nil {
		return nil, err
	}

	return microClient, nil
}
