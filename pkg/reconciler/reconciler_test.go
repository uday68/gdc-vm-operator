package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/ipam"
	"github.com/uday68/gdc-vm-operator/pkg/registry"
	"github.com/uday68/gdc-vm-operator/pkg/runtime"
	"github.com/uday68/gdc-vm-operator/pkg/scheduler"
	"github.com/uday68/gdc-vm-operator/pkg/store"
	"github.com/uday68/gdc-vm-operator/pkg/validator"
)

func setupTestEnvironment(t *testing.T) (*Controller, store.Store, runtime.VMRuntime, *scheduler.Scheduler, *ipam.IPAM, func()) {
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

	ipamMgr, _ := ipam.NewIPAM("10.244.0.0/24")
	sched := scheduler.NewScheduler(16, 32768, 1000)

	ctrl := NewController(s, v, reg, rt, ipamMgr, sched, 500*time.Millisecond)

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return ctrl, s, rt, sched, ipamMgr, cleanup
}

func TestReconciler_ProvisioningAndSelfHealing(t *testing.T) {
	ctrl, s, rt, _, _, cleanup := setupTestEnvironment(t)
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
	ctrl, s, _, _, _, cleanup := setupTestEnvironment(t)
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

func TestReconciler_FailureUnwindsSchedulerAndIPAM(t *testing.T) {
	ctrl, s, _, sched, ipamMgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// 1. Create and register an image properly in registry/images
	testDiskFile, _ := ctrl.registry.GetImagePath("failing-disk.qcow2")
	_ = os.WriteFile(testDiskFile, []byte("disk-content"), 0644)

	_, err := ctrl.registry.RegisterImage("failing-disk-image", "failing-disk.qcow2", "linux", "Failing Disk Test")
	if err != nil {
		t.Fatalf("failed registering test image: %v", err)
	}

	// 2. Now delete the disk binary file so runtime.Start will fail during provisioning!
	_ = os.Remove(testDiskFile)

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "failing-vm"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 4, MemoryMB: 4096, DiskGB: 50},
			Image:     v1alpha1.ImageSource{Name: "failing-disk-image"},
		},
		Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
	}
	_ = s.Save(vm)
	ctx := context.Background()

	// Step 1: Validation passes and reserves capacity
	_ = ctrl.ReconcileOne(ctx, vm)
	v1, _ := s.Get("failing-vm")
	if v1.Status.Phase != v1alpha1.PhaseProvisioning {
		t.Fatalf("expected PhaseProvisioning, got %s: %s", v1.Status.Phase, v1.Status.Message)
	}
	if sched.CapacityReport().AllocatedVCPU != 4 {
		t.Errorf("expected 4 vCPUs allocated, got %d", sched.CapacityReport().AllocatedVCPU)
	}

	// Step 2: Provisioning fails because disk is missing -> MUST unwind capacity & IP
	_ = ctrl.ReconcileOne(ctx, v1)
	v2, _ := s.Get("failing-vm")
	if v2.Status.Phase != v1alpha1.PhaseFailed {
		t.Fatalf("expected PhaseFailed, got %s", v2.Status.Phase)
	}

	// Check unwound scheduler capacity
	cap := sched.CapacityReport()
	if cap.AllocatedVCPU != 0 {
		t.Errorf("regression: scheduler capacity leaked on failure! Expected 0 vCPU, got %d", cap.AllocatedVCPU)
	}

	// Check unwound IPAM
	allocIPs, _ := ipamMgr.Stats()
	if allocIPs != 0 {
		t.Errorf("regression: IPAM IP leaked on failure! Expected 0 allocated IPs, got %d", allocIPs)
	}
}

func TestReconciler_TerminationUnwindsResources(t *testing.T) {
	ctrl, s, rt, sched, ipamMgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "term-vm"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 20},
			Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
		},
		Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
	}
	_ = s.Save(vm)
	ctx := context.Background()

	// Pending -> Provisioning -> Running
	_ = ctrl.ReconcileOne(ctx, vm)
	v1, _ := s.Get("term-vm")
	_ = ctrl.ReconcileOne(ctx, v1)
	v2, _ := s.Get("term-vm")
	if v2.Status.Phase != v1alpha1.PhaseRunning {
		t.Fatalf("expected PhaseRunning, got %s", v2.Status.Phase)
	}

	// Verify resources are committed
	if sched.CapacityReport().AllocatedVCPU != 2 {
		t.Errorf("expected 2 vCPUs allocated")
	}

	// Trigger Termination
	v2.Status.Phase = v1alpha1.PhaseTerminating
	_ = s.Save(v2)
	_ = ctrl.ReconcileOne(ctx, v2)

	// Verify removed from store
	if _, err := s.Get("term-vm"); err == nil {
		t.Errorf("expected VM to be deleted from store")
	}

	// Verify resources unwound
	if sched.CapacityReport().AllocatedVCPU != 0 {
		t.Errorf("expected 0 vCPUs allocated after termination")
	}
	allocIPs, _ := ipamMgr.Stats()
	if allocIPs != 0 {
		t.Errorf("expected 0 IPs allocated after termination")
	}

	// Clean up process if still running
	_ = rt.Stop(v2.Status.PID)
}
