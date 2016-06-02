package platform_grpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/microplatform-io/platform"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var logger = platform.GetLogger("platform-grpc")

type httpEndpointDetails struct {
	Protocol string
	Host     string
	Port     string
}

// HttpEndpointGetter takes a fully formed url such as
// https://trapcall.microplatform.io:4773/server and parses the JSON in the
// response to return craft the gRPC endpoint. The endpoint should return a
// response similar to the following.
//
// {"protocol":"https","host":"173-63-73-14.microplatform.io","port":"4772"}
//
func HttpEndpointGetter(endpoint string) func() string {
	return func() string {
		logger.Printf("Getting the endpoint details from: %s", endpoint)

		resp, err := http.Get(endpoint)
		if err != nil {
			logger.Printf("Failed to receive details from: %s - %s", endpoint, err)

			return ""
		}
		defer resp.Body.Close()

		endpointDetails := &httpEndpointDetails{}
		if err := json.NewDecoder(resp.Body).Decode(endpointDetails); err != nil {
			return ""
		}

		logger.Printf("Got the endpoint details from: %s - %#v", endpoint, endpointDetails)

		return fmt.Sprintf("%s:%s", endpointDetails.Host, endpointDetails.Port)
	}
}

// MicroClientConfig is a struct to help instruct the Client on how to connect.
// The endpoint getter should return an {ADDRESS:PORT} to find the gRPC server.
// If a CertFile and Domain are provided, the client assumes TLS, if CertFile
// and Domain are not provided, the client assumes insecure TCP.
type MicroClientConfig struct {
	EndpointGetter   func() string
	HeartbeatTimeout time.Duration

	// TLS details
	CertFile string
	Domain   string
}

// MicroClientInterface should be used when accepting a MicroClient as a
// parameter so that it can easily be stubbed out for testing.
type MicroClientInterface interface {
	Route(*platform.Request) (chan *platform.Request, chan interface{})
	Reconnect() error
	Close() error
}

type MicroClient struct {
	config                 MicroClientConfig
	clientConn             *grpc.ClientConn
	client                 RouterClient
	transportAuthenticator credentials.TransportAuthenticator
}

func (mc *MicroClient) Close() error {
	if mc.clientConn != nil {
		return mc.clientConn.Close()
	}

	return nil
}

func (mc *MicroClient) Reconnect() error {
	return nil
}

func (mc *MicroClient) connect() (*grpc.ClientConn, error) {
	var dialOptions grpc.DialOption

	if mc.transportAuthenticator != nil {
		logger.Println("> transport authenticator is set, attempting to generate a secure connection to gRPC")
		dialOptions = grpc.WithTransportCredentials(mc.transportAuthenticator)
	} else {
		logger.Println("> transport authenticator is NOT set, attempting to generate an INSECURE connection to gRPC")
		dialOptions = grpc.WithInsecure()
	}

	return grpc.Dial(mc.config.EndpointGetter(), dialOptions)
}

func (mc *MicroClient) getHeartbeatTimeout() time.Duration {
	if mc.config.HeartbeatTimeout > 0*time.Second {
		return mc.config.HeartbeatTimeout
	}

	return 7 * time.Second
}

func (mc *MicroClient) Route(request *platform.Request) (chan *platform.Request, chan interface{}) {
	responses := make(chan *platform.Request, 5)
	streamTimeout := make(chan interface{})

	closed := false

	closeStreamTimeout := func() {
		if closed {
			return
		}

		closed = true

		close(streamTimeout)
	}

	if request == nil {
		closeStreamTimeout()

		return responses, streamTimeout
	}

	if request.Uuid == nil {
		request.Uuid = platform.String(platform.CreateUUID())
	}

	logger.Printf("[MicroClient.Route] %s - creating stream", request.GetUuid())

	stream, err := mc.client.Route(context.Background())
	if err != nil {
		closeStreamTimeout()

		return responses, streamTimeout
	}

	logger.Printf("[MicroClient.Route] %s - created stream", request.GetUuid())

	go func() {
		for {
			logger.Printf("[MicroClient.Route] %s - waiting on response from grpc", request.GetUuid())

			grpcResponse, err := stream.Recv()
			if err != nil {
				closeStreamTimeout()

				logger.Printf("[MicroClient.Route] %s - failed to recv client response: %s", request.GetUuid(), err)
				break
			}

			response := &platform.Request{}
			if err := platform.Unmarshal(grpcResponse.Payload, response); err != nil {
				logger.Printf("[MicroClient.Route] %s - failed to unmarshal platform response: %s", request.GetUuid(), err)
				continue
			}

			logger.Debugf("[MicroClient.Route] %s - received a response", request.GetUuid())
			logger.PrettyPrint(response)

			if response.Routing != nil && response.Routing.RouteTo[0].GetUri() == "resource:///heartbeat" {
				continue
			}

			select {
			case responses <- response:
				logger.Debugf("[MicroClient.Route] %s - successfully placed response on channel", request.GetUuid())
			default:
				logger.Debugf("[MicroClient.Route] %s - failed to place response on channel", request.GetUuid())
			}
		}
	}()

	payload, _ := proto.Marshal(request)

	logger.Printf("[MicroClient.Route] %s - sending request to grpc", request.GetUuid())

	if err := stream.Send(&Request{
		Payload: payload,
	}); err != nil {
		logger.Printf("[MicroClient.Route] failed to send client response: %s", err)
		closeStreamTimeout()

		return responses, streamTimeout
	}

	stream.CloseSend()

	logger.Printf("[MicroClient.Route] %s - sent request to grpc", request.GetUuid())

	return responses, streamTimeout
}

func NewMicroClient(config MicroClientConfig) (*MicroClient, error) {
	if config.EndpointGetter == nil || config.EndpointGetter() == "" {
		return nil, errors.New("Endpoint getter is not properly defined as a function that returns a string")
	}

	transportAuthenticator, err := credentials.NewClientTLSFromFile(config.CertFile, config.Domain)
	if err != nil {
		log.Println("> failed to create transport authenticator!", err)
		log.Println("> WARNING: USING GRPC INSECURELY")
	}

	microClient := &MicroClient{
		config:                 config,
		transportAuthenticator: transportAuthenticator,
	}

	clientConn, err := microClient.connect()
	if err != nil {
		return nil, err
	}

	microClient.clientConn = clientConn
	microClient.client = NewRouterClient(microClient.clientConn)

	return microClient, nil
}
