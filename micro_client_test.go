package platform_grpc

import (
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/microplatform-io/platform"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

type countingListener struct {
	listener net.Listener

	conns            []net.Conn
	totalAcceptCalls int
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.listener.Accept()
	if err == nil {
		l.totalAcceptCalls += 1
		l.conns = append(l.conns, conn)
	}

	return conn, err
}

func (l *countingListener) Addr() net.Addr {
	return l.Addr()
}

func (l *countingListener) Close() error {
	for i := range l.conns {
		l.conns[i].Close()
	}

	return l.listener.Close()
}

type testMicroServer struct {
}

func (s *testMicroServer) Route(routeServer Router_RouteServer) error {
	request, err := routeServer.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}

		return err
	}

	platformRequest := &platform.Request{}
	if err := platform.Unmarshal(request.Payload, platformRequest); err != nil {
		return err
	}

	logger.Debugf("[testGrpcServer] %s - got a request, sending a response now", platformRequest.GetUuid())

	if err := routeServer.Send(request); err != nil {
		logger.Debugf("[testGrpcServer] %s - failed to send response: %s", platformRequest.GetUuid(), err)

		return err
	}

	logger.Debugf("[testGrpcServer] %s - got a request, sent response", platformRequest.GetUuid())

	return nil
}

func newTestMicroServer() *testMicroServer {
	return &testMicroServer{}
}

type testGrpcServer struct {
	URL      string
	Listener net.Listener

	testMicroServer *testMicroServer
	grpcServer      *grpc.Server
}

func newTestGrpcServer() *testGrpcServer {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// It's very important to use unique ports to prevent binding to a used port
	port := strconv.Itoa(8000 + (r.Int() % 2000))
	addr := ":" + port

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	lis = &countingListener{
		listener:         lis,
		totalAcceptCalls: 0,
	}

	testMicroServer := newTestMicroServer()

	s := grpc.NewServer()
	RegisterRouterServer(s, testMicroServer)
	go s.Serve(lis)

	return &testGrpcServer{
		URL:      addr,
		Listener: lis,

		testMicroServer: testMicroServer,
		grpcServer:      s,
	}
}

func TestNewMicroClient(t *testing.T) {
	Convey("A micro client config that does not return an endpoint should fail", t, func() {
		microClient, err := NewMicroClient(MicroClientConfig{})
		So(microClient, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})

	Convey("A micro client that has a valid endpoint getter should return successfully", t, func() {
		microClient, err := NewMicroClient(MicroClientConfig{
			EndpointGetter: func() string { return "whatever:8000" },
		})
		defer microClient.Close()

		So(microClient, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})
}

func TestMicroClientRoute(t *testing.T) {
	Convey("Routing a nil request should produce a stream timeout", t, func() {
		microClient, err := NewMicroClient(MicroClientConfig{
			EndpointGetter:   func() string { return "whatever:8000" },
			HeartbeatTimeout: 100 * time.Millisecond,
		})
		defer microClient.Close()
		So(microClient, ShouldNotBeNil)
		So(err, ShouldBeNil)

		responses, streamTimeout := microClient.Route(nil)
		select {
		case response := <-responses:
			t.Errorf("got an unexpected response from routing: %#v", response)
		case <-streamTimeout:
			// Good to go!
		}
	})

	// Convey("Routing a request on a client with an invalid endpoint should produce a stream timeout", t, func() {
	// 	microClient, err := NewMicroClient(MicroClientConfig{
	// 		EndpointGetter:   func() string { return "whatever:8000" },
	// 		HeartbeatTimeout: 100 * time.Millisecond,
	// 	})
	// 	defer microClient.Close()
	// 	So(microClient, ShouldNotBeNil)
	// 	So(err, ShouldBeNil)

	// 	responses, streamTimeout := microClient.Route(&platform.Request{})
	// 	select {
	// 	case response := <-responses:
	// 		t.Errorf("got an unexpected response from routing: %#v", response)
	// 	case <-streamTimeout:
	// 		// Good to go!
	// 	}
	// })

	Convey("Routing a request on a client to the test server should echo back the request", t, func() {
		testGrpcServer := newTestGrpcServer()
		defer testGrpcServer.grpcServer.Stop()

		microClient, err := NewMicroClient(MicroClientConfig{
			EndpointGetter:   func() string { return testGrpcServer.URL },
			HeartbeatTimeout: 100 * time.Millisecond,
		})
		defer microClient.Close()
		So(microClient, ShouldNotBeNil)
		So(err, ShouldBeNil)

		request := &platform.Request{
			Routing:   platform.RouteToUri("resource:///platform/reply/testing"),
			Payload:   []byte("HELLO"),
			Completed: platform.Bool(true),
		}

		responses, streamTimeout := microClient.Route(request)
		select {
		case response := <-responses:
			So(response, ShouldResemble, request)
		case <-streamTimeout:
			t.Errorf("got an unexpected stream timeout from routing")
		}
	})

	Convey("Routing many requests should not produce any locks", t, func(c C) {
		testGrpcServer := newTestGrpcServer()
		defer testGrpcServer.grpcServer.Stop()

		microClient, err := NewMicroClient(MicroClientConfig{
			EndpointGetter:   func() string { return testGrpcServer.URL },
			HeartbeatTimeout: 100 * time.Millisecond,
		})
		defer microClient.Close()
		So(microClient, ShouldNotBeNil)
		So(err, ShouldBeNil)

		wg := sync.WaitGroup{}

		for i := 0; i < 10000; i++ {
			wg.Add(1)

			go func() {
				request := &platform.Request{
					Routing:   platform.RouteToUri("resource:///platform/reply/testing"),
					Payload:   []byte("HELLO"),
					Completed: platform.Bool(true),
				}

				startTime := time.Now()

				responses, streamTimeout := microClient.Route(request)
				select {
				case response := <-responses:
					c.So(response, ShouldResemble, request)
				case <-streamTimeout:
					t.Errorf("%s - got an unexpected stream timeout from routing: %s", request.GetUuid(), time.Now().Sub(startTime))
				}

				wg.Done()
			}()
		}

		wg.Wait()
	})
}
