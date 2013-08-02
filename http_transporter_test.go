package raft

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Ensure that we can start several servers and have them communicate.
func TestHTTPTransporter(t *testing.T) {
	transporter := NewHTTPTransporter("/raft")
	transporter.DisableKeepAlives = true

	servers := []*Server{}
	f0 := func(server *Server, httpServer *http.Server) {
		// Stop the leader and wait for an election.
		server.Stop()
		time.Sleep(testElectionTimeout * 2)

		if servers[1].State() != Leader && servers[2].State() != Leader {
			t.Fatal("Expected re-election:", servers[1].State(), servers[2].State())
		}
		server.Start()
	}
	runTestHttpServers(&servers, transporter, f0, nopServer, nopServer)
}

func BenchmarkHttpTransporter(b *testing.B) {
	transporter := NewHTTPTransporter("/raft")
	transporter.DisableKeepAlives = true
	numClients, numMessagesPerClient := 100, 100

	servers := []*Server{}
	f0 := func(server *Server, httpServer *http.Server) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			for j := 0; j<numClients; j++ {
				wg.Add(1)
				go func() {
					for k := 0; k<numMessagesPerClient; k++ {
						server.Do(NOPCommand{})
					}
					wg.Done()
				}()
			}
			wg.Wait()
		}
	}
	runTestHttpServers(&servers, transporter, f0, nopServer, nopServer)
}

// Starts multiple independent Raft servers wrapped with HTTP servers.
func runTestHttpServers(servers *[]*Server, transporter *HTTPTransporter, callbacks ...func(*Server, *http.Server)) {
	var wg sync.WaitGroup
	httpServers := []*http.Server{}
	listeners := []net.Listener{}
	for i, _ := range callbacks {
		wg.Add(1)
		port := 9000 + i

		// Create raft server.
		server := newTestServer(fmt.Sprintf("localhost:%d", port), transporter)
		server.SetHeartbeatTimeout(testHeartbeatTimeout)
		server.SetElectionTimeout(testElectionTimeout)
		server.Start()

		defer server.Stop()
		*servers = append(*servers, server)

		// Create listener for HTTP server and start it.
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			panic(err)
		}
		defer listener.Close()
		listeners = append(listeners, listener)

		// Create wrapping HTTP server.
		mux := http.NewServeMux()
		transporter.Install(server, mux)
		httpServer := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
		httpServers = append(httpServers, httpServer)
		go func() { httpServer.Serve(listener) }()
	}

	// Setup configuration.
	for _, server := range *servers {
		if _, err := (*servers)[0].Do(&DefaultJoinCommand{Name: server.Name()}); err != nil {
			panic(fmt.Sprintf("Server %s unable to join: %v", server.Name(), err))
		}
	}

	// Wait for configuration to propagate.
	time.Sleep(testHeartbeatTimeout * 2)

	// Execute all the callbacks at the same time.
	for _i, _f := range callbacks {
		i, f := _i, _f
		go func() {
			defer wg.Done()
			f((*servers)[i], httpServers[i])
		}()
	}

	// Wait until everything is done.
	wg.Wait()
}

func nopServer(server *Server, httpServer *http.Server) {
}

