package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/store"
)

// Server provides liveness, readiness, and Prometheus metric endpoints.
type Server struct {
	port       int
	store      store.Store
	httpServer *http.Server
	mu         sync.RWMutex
}

// NewServer initializes the telemetry HTTP server.
func NewServer(port int, s store.Store) *Server {
	if port <= 0 {
		port = 8080
	}
	srv := &Server{
		port:  port,
		store: s,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.handleHealthz)
	mux.HandleFunc("/readyz", srv.handleReadyz)
	mux.HandleFunc("/metrics", srv.handleMetrics)

	srv.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return srv
}

// Start launches the telemetry server in the background.
func (s *Server) Start(ctx context.Context) error {
	slog.Info("Starting operator observability telemetry server", "port", s.port)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Observability server error", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(shutdownCtx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":    "healthy",
		"component": "gdc-vm-operator",
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vms := s.store.List()
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "ready",
		"managed_vms": len(vms),
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	vms := s.store.List()

	total := len(vms)
	running := 0
	failed := 0
	degraded := 0
	totalRestarts := 0

	for _, vm := range vms {
		switch vm.Status.Phase {
		case v1alpha1.PhaseRunning:
			running++
		case v1alpha1.PhaseFailed:
			failed++
		case v1alpha1.PhaseDegraded:
			degraded++
		}
		totalRestarts += vm.Status.RestartCount
	}

	fmt.Fprintf(w, "# HELP gdc_vms_total Total number of virtual machines tracked\n")
	fmt.Fprintf(w, "# TYPE gdc_vms_total gauge\n")
	fmt.Fprintf(w, "gdc_vms_total %d\n\n", total)

	fmt.Fprintf(w, "# HELP gdc_vms_running Number of virtual machines currently active\n")
	fmt.Fprintf(w, "# TYPE gdc_vms_running gauge\n")
	fmt.Fprintf(w, "gdc_vms_running %d\n\n", running)

	fmt.Fprintf(w, "# HELP gdc_vms_failed Number of virtual machines in failed state\n")
	fmt.Fprintf(w, "# TYPE gdc_vms_failed gauge\n")
	fmt.Fprintf(w, "gdc_vms_failed %d\n\n", failed)

	fmt.Fprintf(w, "# HELP gdc_vms_degraded Number of virtual machines in crashloop degraded state\n")
	fmt.Fprintf(w, "# TYPE gdc_vms_degraded gauge\n")
	fmt.Fprintf(w, "gdc_vms_degraded %d\n\n", degraded)

	fmt.Fprintf(w, "# HELP gdc_vm_restarts_total Cumulative self-healed process restart count\n")
	fmt.Fprintf(w, "# TYPE gdc_vm_restarts_total counter\n")
	fmt.Fprintf(w, "gdc_vm_restarts_total %d\n", totalRestarts)
}

