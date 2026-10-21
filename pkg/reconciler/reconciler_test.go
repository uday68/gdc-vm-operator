package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/registry"
	"github.com/uday68/gdc-vm-operator/pkg/runtime"
	"github.com/uday68/gdc-vm-operator/pkg/store"
	"github.com/uday68/gdc-vm-operator/pkg/validator"
)

func setupTestEnvironment(t *testing.T) (*Controller, store.Store, runtime.VMRuntime, func()) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "gdc-reconciler-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	regDir := filepath.Join(tempDir, "registry")
	reg, err := registry.NewLocalRegistry(regDir)
	if err != nil {
		t.Fatalf("failed to init registry: %v", err)
	}

	// Create dummy image in registry
	dummyPath := filepath.Join(regDir, "images", "ubuntu-minimal.qcow2")
	_ = os.WriteFile(dummyPath, []byte("mock-image-bytes"), 0644)
	_, _ = reg.RegisterImage("ubuntu-minimal", "ubuntu-minimal.qcow2", "linux", "Ubuntu Minimal Test Image")

	// Store
	storeDir := filepath.Join(tempDir, "store")
	s, err := store.NewFileStore(storeDir)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}

	// Validator
	v := validator.NewValidator(reg, nil)

	// Runtime
	absRunnerPath, _ := filepath.Abs("../../bin/vm-runner.exe")
	logsDir := filepath.Join(tempDir, "logs")
	rt, err := runtime.NewProcessVMRuntime(logsDir, absRunnerPath)
	if err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}

	ctrl := NewController(s, v, reg, rt, 500*time.Millisecond)

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return ctrl, s, rt, cleanup
}

func TestReconciler_ProvisioningAndSelfHealing(t *testing.T) {
	ctrl, s, rt, cleanup := setupTestEnvironment(t)
	defer cleanup()

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "worker-vm-01"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources:     v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 256, DiskGB: 10},
			Image:         v1alpha1.ImageSource{Name: "ubuntu-minimal"},
			RestartPolicy: v1alpha1.RestartPolicyAlways,
		},
		Status: v1alpha1.VirtualMachineStatus{
			Phase: v1alpha1.PhasePending,
		},
	}

	if err := s.Save(vm); err != nil {
		t.Fatalf("failed to save VM: %v", err)
	}

	ctx := context.Background()

	// Pass 1: Pending -> Provisioning (Validation)
	if err := ctrl.ReconcileOne(ctx, vm); err != nil {
		t.Fatalf("reconcile pass 1 failed: %v", err)
	}
	v1, _ := s.Get("worker-vm-01")
	if v1.Status.Phase != v1alpha1.PhaseProvisioning {
		t.Fatalf("expected PhaseProvisioning, got %s", v1.Status.Phase)
	}

	// Pass 2: Provisioning -> Running (Process Start)
	if err := ctrl.ReconcileOne(ctx, v1); err != nil {
		t.Fatalf("reconcile pass 2 failed: %v", err)
	}
	v2, _ := s.Get("worker-vm-01")
	if v2.Status.Phase != v1alpha1.PhaseRunning {
		t.Fatalf("expected PhaseRunning, got %s", v2.Status.Phase)
	}
	if v2.Status.PID <= 0 {
		t.Fatalf("expected valid PID, got %d", v2.Status.PID)
	}
	initialPID := v2.Status.PID

	// Verify process is alive
	if !rt.IsAlive(initialPID) {
		t.Fatalf("expected PID %d to be alive", initialPID)
	}

	// Now simulate a crash: forcefully kill the process!
	_ = rt.Stop(initialPID)
	time.Sleep(500 * time.Millisecond)

	// Pass 3: Self-healing detection & auto-restart!
	if err := ctrl.ReconcileOne(ctx, v2); err != nil {
		t.Fatalf("reconcile pass 3 failed: %v", err)
	}
	v3, _ := s.Get("worker-vm-01")
	if v3.Status.Phase != v1alpha1.PhaseRunning {
		t.Fatalf("expected PhaseRunning after self-healing, got %s", v3.Status.Phase)
	}
	if v3.Status.RestartCount != 1 {
		t.Errorf("expected RestartCount == 1, got %d", v3.Status.RestartCount)
	}
	if v3.Status.PID == initialPID {
		t.Errorf("expected new PID after restart, got same PID: %d", v3.Status.PID)
	}

	// Clean up spawned process
	_ = rt.Stop(v3.Status.PID)
}

func TestReconciler_AirGapRejection(t *testing.T) {
	ctrl, s, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "bad-url-vm"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 256, DiskGB: 10},
			Image: v1alpha1.ImageSource{
				Name:      "ubuntu-minimal",
				RemoteURL: "https://evil-unauthorized-site.com/image.iso",
			},
		},
		Status: v1alpha1.VirtualMachineStatus{
			Phase: v1alpha1.PhasePending,
		},
	}

	_ = s.Save(vm)
	ctx := context.Background()

	_ = ctrl.ReconcileOne(ctx, vm)

	v, _ := s.Get("bad-url-vm")
	if v.Status.Phase != v1alpha1.PhaseFailed {
		t.Fatalf("expected PhaseFailed for air-gap violation, got %s", v.Status.Phase)
	}
}
