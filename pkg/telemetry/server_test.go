package telemetry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/store"
)

func TestTelemetryServer_Endpoints(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-telemetry-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	s, err := store.NewFileStore(tempDir)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}

	// Add sample VM
	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "telemetry-vm"},
		Status: v1alpha1.VirtualMachineStatus{
			Phase:        v1alpha1.PhaseRunning,
			RestartCount: 2,
		},
	}
	_ = s.Save(vm)

	srv := NewServer(8080, s)

	// 1. Test /healthz
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rrHealth := httptest.NewRecorder()
	srv.handleHealthz(rrHealth, reqHealth)

	if rrHealth.Code != http.StatusOK {
		t.Errorf("expected 200 from /healthz, got %d", rrHealth.Code)
	}
	var healthBody map[string]string
	_ = json.NewDecoder(rrHealth.Body).Decode(&healthBody)
	if healthBody["status"] != "healthy" {
		t.Errorf("expected healthy, got %s", healthBody["status"])
	}

	// 2. Test /readyz
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rrReady := httptest.NewRecorder()
	srv.handleReadyz(rrReady, reqReady)

	if rrReady.Code != http.StatusOK {
		t.Errorf("expected 200 from /readyz, got %d", rrReady.Code)
	}

	// 3. Test /metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rrMetrics := httptest.NewRecorder()
	srv.handleMetrics(rrMetrics, reqMetrics)

	if rrMetrics.Code != http.StatusOK {
		t.Errorf("expected 200 from /metrics, got %d", rrMetrics.Code)
	}
	bodyStr := rrMetrics.Body.String()
	if !strings.Contains(bodyStr, "gdc_vms_running 1") {
		t.Errorf("expected metric 'gdc_vms_running 1', got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "gdc_vm_restarts_total 2") {
		t.Errorf("expected metric 'gdc_vm_restarts_total 2', got:\n%s", bodyStr)
	}
}
