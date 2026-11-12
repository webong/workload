// A dependency-free HTTP fixture for real Docker and Kubernetes tests. Each
// process generates a boot ID so recovery can prove adoption through HTTP.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		log.Fatal(err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	identity := struct{ BootID, Hostname, Revision, Marker string }{hex.EncodeToString(nonce[:]), hostname, os.Getenv("WORKLOAD_FIXTURE_REVISION"), os.Getenv("WORKLOAD_FIXTURE_MARKER")}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/identity", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(identity)
	})
	servers := []*http.Server{}
	for _, address := range []string{":8080", ":9090"} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			log.Fatal(err)
		}
		server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
		servers = append(servers, server)
		go func() {
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				log.Fatal(err)
			}
		}()
	}
	log.Printf("fixture ready: boot=%s revision=%s", identity.BootID, identity.Revision)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	<-ctx.Done()
	for _, server := range servers {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = server.Shutdown(ctx)
		cancel()
	}
}
