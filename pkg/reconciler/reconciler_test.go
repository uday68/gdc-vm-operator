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

// mockRuntime allows deterministic testing of runtime failures, exit codes, and restart policies
type mockRuntime struct {
	aliveMap    map[int]bool
	exitCodes   map[string]int
	exitKnown   map[string]bool
	nextPID     int
	startErr    error
	stoppedPIDs []int
}

func newMockRuntime() *mockRuntime {
	return &mockRuntime{
		aliveMap:  make(map[int]bool),
		exitCodes: make(map[string]int),
		exitKnown: make(map[string]bool),
		nextPID:   1000,
	}
}

func (m *mockRuntime) Start(ctx context.Context, vm *v1alpha1.VirtualMachine, imagePath string) (int, error) {
	if m.startErr != nil {
		return 0, m.startErr
	}
	m.nextPID++
	m.aliveMap[m.nextPID] = true
	return m.nextPID, nil
}

func (m *mockRuntime) Stop(pid int) error {
	m.aliveMap[pid] = false
	m.stoppedPIDs = append(m.stoppedPIDs, pid)
	return nil
}

func (m *mockRuntime) IsAlive(pid int) bool {
	return m.aliveMap[pid]
}

func (m *mockRuntime) GetExitCode(vmName string) (int, bool) {
	code := m.exitCodes[vmName]
	known := m.exitKnown[vmName]
	return code, known
}

func (m *mockRuntime) GetLogs(vmName string) (string, error) {
	return "mock logs", nil
}

func TestReconciler_RestartPolicies(t *testing.T) {
	ctx := context.Background()

	t.Run("RestartPolicy Never with clean exit (0) transitions to Stopped", func(t *testing.T) {
		ctrl, s, _, sched, ipamMgr, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		ctrl.runtime = mock

		vm := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "never-clean-vm"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources:     v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 10},
				Image:         v1alpha1.ImageSource{Name: "ubuntu-minimal"},
				RestartPolicy: v1alpha1.RestartPolicyNever,
			},
			Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
		}
		_ = s.Save(vm)

		// Pending -> Provisioning -> Running
		_ = ctrl.ReconcileOne(ctx, vm)
		v1, _ := s.Get("never-clean-vm")
		_ = ctrl.ReconcileOne(ctx, v1)
		v2, _ := s.Get("never-clean-vm")
		if v2.Status.Phase != v1alpha1.PhaseRunning {
			t.Fatalf("expected PhaseRunning, got: %s", v2.Status.Phase)
		}

		// Simulate clean exit
		mock.aliveMap[v2.Status.PID] = false
		mock.exitCodes["never-clean-vm"] = 0
		mock.exitKnown["never-clean-vm"] = true

		_ = ctrl.ReconcileOne(ctx, v2)
		v3, _ := s.Get("never-clean-vm")
		if v3.Status.Phase != v1alpha1.PhaseStopped {
			t.Fatalf("expected PhaseStopped, got: %s", v3.Status.Phase)
		}
		if sched.CapacityReport().AllocatedVCPU != 0 {
			t.Errorf("expected 0 vCPUs allocated after clean stop")
		}
		allocIPs, _ := ipamMgr.Stats()
		if allocIPs != 0 {
			t.Errorf("expected 0 IPs allocated after clean stop")
		}
	})

	t.Run("RestartPolicy Never with crash exit (1) transitions to Failed", func(t *testing.T) {
		ctrl, s, _, sched, ipamMgr, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		ctrl.runtime = mock

		vm := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "never-crash-vm"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources:     v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 10},
				Image:         v1alpha1.ImageSource{Name: "ubuntu-minimal"},
				RestartPolicy: v1alpha1.RestartPolicyNever,
			},
			Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
		}
		_ = s.Save(vm)

		_ = ctrl.ReconcileOne(ctx, vm)
		v1, _ := s.Get("never-crash-vm")
		_ = ctrl.ReconcileOne(ctx, v1)
		v2, _ := s.Get("never-crash-vm")

		// Simulate crash exit
		mock.aliveMap[v2.Status.PID] = false
		mock.exitCodes["never-crash-vm"] = 1
		mock.exitKnown["never-crash-vm"] = true

		_ = ctrl.ReconcileOne(ctx, v2)
		v3, _ := s.Get("never-crash-vm")
		if v3.Status.Phase != v1alpha1.PhaseFailed {
			t.Fatalf("expected PhaseFailed, got: %s", v3.Status.Phase)
		}
		if sched.CapacityReport().AllocatedVCPU != 0 {
			t.Errorf("expected 0 vCPUs allocated after crash failure")
		}
		allocIPs, _ := ipamMgr.Stats()
		if allocIPs != 0 {
			t.Errorf("expected 0 IPs allocated after crash failure")
		}
	})

	t.Run("RestartPolicy OnFailure with clean exit (0) does not restart", func(t *testing.T) {
		ctrl, s, _, _, _, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		ctrl.runtime = mock

		vm := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "onfailure-clean-vm"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources:     v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 1024, DiskGB: 10},
				Image:         v1alpha1.ImageSource{Name: "ubuntu-minimal"},
				RestartPolicy: v1alpha1.RestartPolicyOnFailure,
			},
			Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
		}
		_ = s.Save(vm)

		_ = ctrl.ReconcileOne(ctx, vm)
		v1, _ := s.Get("onfailure-clean-vm")
		_ = ctrl.ReconcileOne(ctx, v1)
		v2, _ := s.Get("onfailure-clean-vm")

		mock.aliveMap[v2.Status.PID] = false
		mock.exitCodes["onfailure-clean-vm"] = 0
		mock.exitKnown["onfailure-clean-vm"] = true

		_ = ctrl.ReconcileOne(ctx, v2)
		v3, _ := s.Get("onfailure-clean-vm")
		if v3.Status.Phase != v1alpha1.PhaseStopped {
			t.Fatalf("expected PhaseStopped for clean OnFailure exit, got: %s", v3.Status.Phase)
		}
		if v3.Status.RestartCount != 0 {
			t.Errorf("expected 0 restarts for clean exit, got: %d", v3.Status.RestartCount)
		}
	})

	t.Run("RestartPolicy OnFailure with crash exit (1) triggers self-healing restart", func(t *testing.T) {
		ctrl, s, _, _, _, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		ctrl.runtime = mock

		vm := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "onfailure-crash-vm"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources:     v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 1024, DiskGB: 10},
				Image:         v1alpha1.ImageSource{Name: "ubuntu-minimal"},
				RestartPolicy: v1alpha1.RestartPolicyOnFailure,
			},
			Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
		}
		_ = s.Save(vm)

		_ = ctrl.ReconcileOne(ctx, vm)
		v1, _ := s.Get("onfailure-crash-vm")
		_ = ctrl.ReconcileOne(ctx, v1)
		v2, _ := s.Get("onfailure-crash-vm")
		oldPID := v2.Status.PID

		mock.aliveMap[oldPID] = false
		mock.exitCodes["onfailure-crash-vm"] = 1
		mock.exitKnown["onfailure-crash-vm"] = true

		_ = ctrl.ReconcileOne(ctx, v2)
		v3, _ := s.Get("onfailure-crash-vm")
		if v3.Status.Phase != v1alpha1.PhaseRunning {
			t.Fatalf("expected PhaseRunning after self-healing restart, got: %s", v3.Status.Phase)
		}
		if v3.Status.RestartCount != 1 {
			t.Errorf("expected RestartCount=1, got: %d", v3.Status.RestartCount)
		}
		if v3.Status.PID == oldPID {
			t.Errorf("expected new PID assigned after restart, got same PID %d", oldPID)
		}
	})

	t.Run("MaxCrashRestarts triggers CrashLoopBackoff and Degraded phase", func(t *testing.T) {
		ctrl, s, _, _, _, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		ctrl.runtime = mock

		vm := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "crashloop-vm"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources:     v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 1024, DiskGB: 10},
				Image:         v1alpha1.ImageSource{Name: "ubuntu-minimal"},
				RestartPolicy: v1alpha1.RestartPolicyAlways,
			},
			Status: v1alpha1.VirtualMachineStatus{
				Phase:        v1alpha1.PhaseRunning,
				PID:          9999,
				RestartCount: 3, // Already hit threshold
			},
		}
		_ = s.Save(vm)

		mock.aliveMap[9999] = false
		mock.exitCodes["crashloop-vm"] = 1
		mock.exitKnown["crashloop-vm"] = true

		_ = ctrl.ReconcileOne(ctx, vm)
		vDegraded, _ := s.Get("crashloop-vm")
		if vDegraded.Status.Phase != v1alpha1.PhaseDegraded {
			t.Fatalf("expected PhaseDegraded after hitting max restarts, got: %s", vDegraded.Status.Phase)
		}
	})
}

func TestReconciler_StartupRecovery(t *testing.T) {
	ctx := context.Background()

	t.Run("Consistent recovery reconstructs scheduler and IPAM without double allocation", func(t *testing.T) {
		ctrl, s, _, _, _, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		ctrl.runtime = mock

		// 1. Run two VMs
		vm1 := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "rec-vm-1"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 20},
				Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
			},
			Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
		}
		vm2 := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "rec-vm-2"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources: v1alpha1.ResourceRequirements{VCPU: 4, MemoryMB: 4096, DiskGB: 40},
				Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
			},
			Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
		}
		_ = s.Save(vm1)
		_ = s.Save(vm2)

		_ = ctrl.ReconcileOne(ctx, vm1)
		v1, _ := s.Get("rec-vm-1")
		_ = ctrl.ReconcileOne(ctx, v1)

		_ = ctrl.ReconcileOne(ctx, vm2)
		v2, _ := s.Get("rec-vm-2")
		_ = ctrl.ReconcileOne(ctx, v2)

		v1Final, _ := s.Get("rec-vm-1")
		v2Final, _ := s.Get("rec-vm-2")
		if v1Final.Status.Phase != v1alpha1.PhaseRunning || v2Final.Status.Phase != v1alpha1.PhaseRunning {
			t.Fatalf("expected both VMs Running")
		}

		ip1 := v1Final.Status.AllocatedIP
		ip2 := v2Final.Status.AllocatedIP

		// 2. Simulate controller crash and restart with fresh Scheduler and IPAM
		freshSched := scheduler.NewScheduler(16, 32768, 1000)
		freshIPAM, _ := ipam.NewIPAM("10.244.0.0/24")
		newCtrl := NewController(s, ctrl.validator, ctrl.registry, mock, freshIPAM, freshSched, 500*time.Millisecond)

		// 3. Reconstruct recovered state
		if err := newCtrl.ReconcileRecoveredState(ctx); err != nil {
			t.Fatalf("ReconcileRecoveredState failed: %v", err)
		}

		// 4. Assert scheduler accounting
		cap := freshSched.CapacityReport()
		if cap.AllocatedVCPU != 6 {
			t.Fatalf("expected 6 vCPUs (2+4) allocated after recovery, got: %d", cap.AllocatedVCPU)
		}
		if cap.AvailableVCPU != 10 {
			t.Fatalf("expected 10 vCPUs available, got: %d", cap.AvailableVCPU)
		}

		// 5. Assert IPAM accounting retains exact previous IPs
		allocIPs, _ := freshIPAM.Stats()
		if allocIPs != 2 {
			t.Fatalf("expected 2 IPs allocated in IPAM, got: %d", allocIPs)
		}
		recIP1, exists1 := freshIPAM.GetAllocatedIP("rec-vm-1")
		recIP2, exists2 := freshIPAM.GetAllocatedIP("rec-vm-2")
		if !exists1 || recIP1 != ip1 {
			t.Errorf("expected vm-1 to retain IP %s, got %s (exists=%v)", ip1, recIP1, exists1)
		}
		if !exists2 || recIP2 != ip2 {
			t.Errorf("expected vm-2 to retain IP %s, got %s (exists=%v)", ip2, recIP2, exists2)
		}
	})

	t.Run("IP conflict detection during recovery flags Degraded", func(t *testing.T) {
		ctrl, s, _, _, _, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		mock.aliveMap[1001] = true
		mock.aliveMap[1002] = true

		// Persist two VMs that claim the same IP
		vm1 := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "conflict-vm-1"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 1024, DiskGB: 10},
				Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
			},
			Status: v1alpha1.VirtualMachineStatus{
				Phase:       v1alpha1.PhaseRunning,
				PID:         1001,
				AllocatedIP: "10.244.0.15",
			},
		}
		vm2 := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "conflict-vm-2"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 1024, DiskGB: 10},
				Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
			},
			Status: v1alpha1.VirtualMachineStatus{
				Phase:       v1alpha1.PhaseRunning,
				PID:         1002,
				AllocatedIP: "10.244.0.15", // Same IP collision!
			},
		}
		_ = s.Save(vm1)
		_ = s.Save(vm2)

		freshSched := scheduler.NewScheduler(16, 32768, 1000)
		freshIPAM, _ := ipam.NewIPAM("10.244.0.0/24")
		newCtrl := NewController(s, ctrl.validator, ctrl.registry, mock, freshIPAM, freshSched, 500*time.Millisecond)

		_ = newCtrl.ReconcileRecoveredState(ctx)

		res1, _ := s.Get("conflict-vm-1")
		res2, _ := s.Get("conflict-vm-2")

		// One is Running, the conflicting one must be Degraded
		if res1.Status.Phase == v1alpha1.PhaseRunning && res2.Status.Phase != v1alpha1.PhaseDegraded {
			t.Fatalf("expected conflict-vm-2 to be Degraded, got: %s", res2.Status.Phase)
		}
		if res2.Status.Phase == v1alpha1.PhaseDegraded {
			cond := res2.GetCondition("NetworkReady")
			if cond == nil || cond.Reason != "IPConflictDetected" {
				t.Fatalf("expected IPConflictDetected condition on conflicting VM")
			}
		}
	})

	t.Run("Capacity overcommit during recovery flags Degraded", func(t *testing.T) {
		ctrl, s, _, _, _, cleanup := setupTestEnvironment(t)
		defer cleanup()

		mock := newMockRuntime()
		mock.aliveMap[2001] = true
		mock.aliveMap[2002] = true

		// Persist two VMs requesting 3 vCPUs each on a 4-vCPU node
		vm1 := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "overcommit-vm-1"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources: v1alpha1.ResourceRequirements{VCPU: 3, MemoryMB: 1024, DiskGB: 10},
				Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
			},
			Status: v1alpha1.VirtualMachineStatus{
				Phase: v1alpha1.PhaseRunning,
				PID:   2001,
			},
		}
		vm2 := &v1alpha1.VirtualMachine{
			Metadata: v1alpha1.ObjectMeta{Name: "overcommit-vm-2"},
			Spec: v1alpha1.VirtualMachineSpec{
				Resources: v1alpha1.ResourceRequirements{VCPU: 3, MemoryMB: 1024, DiskGB: 10},
				Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
			},
			Status: v1alpha1.VirtualMachineStatus{
				Phase: v1alpha1.PhaseRunning,
				PID:   2002,
			},
		}
		_ = s.Save(vm1)
		_ = s.Save(vm2)

		tinySched := scheduler.NewScheduler(4, 32768, 1000)
		tinyIPAM, _ := ipam.NewIPAM("10.244.0.0/24")
		newCtrl := NewController(s, ctrl.validator, ctrl.registry, mock, tinyIPAM, tinySched, 500*time.Millisecond)

		_ = newCtrl.ReconcileRecoveredState(ctx)

		res2, _ := s.Get("overcommit-vm-2")
		if res2.Status.Phase != v1alpha1.PhaseDegraded {
			t.Fatalf("expected overcommit-vm-2 to be Degraded, got: %s", res2.Status.Phase)
		}
		cond := res2.GetCondition("Scheduled")
		if cond == nil || cond.Reason != "RecoveryCapacityExceeded" {
			t.Fatalf("expected RecoveryCapacityExceeded condition, got: %+v", cond)
		}
	})
}

func TestReconciler_SpecUpdateWhileRunning(t *testing.T) {
	ctx := context.Background()
	ctrl, s, _, sched, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	mock := newMockRuntime()
	ctrl.runtime = mock

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "dynamic-scale-vm"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 20},
			Image:     v1alpha1.ImageSource{Name: "ubuntu-minimal"},
		},
		Status: v1alpha1.VirtualMachineStatus{Phase: v1alpha1.PhasePending},
	}
	_ = s.Save(vm)

	_ = ctrl.ReconcileOne(ctx, vm)
	v1, _ := s.Get("dynamic-scale-vm")
	_ = ctrl.ReconcileOne(ctx, v1)
	v2, _ := s.Get("dynamic-scale-vm")
	if v2.Status.Phase != v1alpha1.PhaseRunning {
		t.Fatalf("expected PhaseRunning, got: %s", v2.Status.Phase)
	}
	if sched.CapacityReport().AllocatedVCPU != 2 {
		t.Fatalf("expected 2 vCPUs initially allocated")
	}

	// 1. Valid update: scale to 4 vCPUs
	v2.Spec.Resources.VCPU = 4
	_ = s.Save(v2)
	_ = ctrl.ReconcileOne(ctx, v2)

	if sched.CapacityReport().AllocatedVCPU != 4 {
		t.Fatalf("expected 4 vCPUs allocated after spec update, got: %d", sched.CapacityReport().AllocatedVCPU)
	}
	v3, _ := s.Get("dynamic-scale-vm")
	cond := v3.GetCondition("Scheduled")
	if cond == nil || cond.Reason != "SpecUpdateApplied" {
		t.Fatalf("expected SpecUpdateApplied condition, got: %+v", cond)
	}

	// 2. Invalid update: scale to 32 vCPUs (exceeds node capacity of 16)
	v3.Spec.Resources.VCPU = 32
	_ = s.Save(v3)
	_ = ctrl.ReconcileOne(ctx, v3)

	// Verify previous valid reservation (4 vCPUs) is preserved
	if sched.CapacityReport().AllocatedVCPU != 4 {
		t.Fatalf("previous reservation lost! Expected 4 vCPUs, got: %d", sched.CapacityReport().AllocatedVCPU)
	}
	v4, _ := s.Get("dynamic-scale-vm")
	cond2 := v4.GetCondition("Scheduled")
	if cond2 == nil || cond2.Reason != "SpecUpdateFailed" {
		t.Fatalf("expected SpecUpdateFailed condition, got: %+v", cond2)
	}
}

