package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
)

func TestProcessVMRuntime_Lifecycle(t *testing.T) {
	tempLogsDir, err := os.MkdirTemp("", "gdc-runtime-logs-*")
	if err != nil {
		t.Fatalf("failed to create temp logs dir: %v", err)
	}
	defer os.RemoveAll(tempLogsDir)

	// Check if bin/vm-runner.exe exists, locate absolute path
	absRunnerPath, err := filepath.Abs("../../bin/vm-runner.exe")
	if err != nil {
		t.Fatalf("failed to resolve runner path: %v", err)
	}

	rt, err := NewProcessVMRuntime(tempLogsDir, absRunnerPath)
	if err != nil {
		t.Fatalf("failed to initialize runtime: %v", err)
	}

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "test-runtime-vm"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 256, DiskGB: 10},
		},
	}

	dummyImagePath := filepath.Join(tempLogsDir, "dummy-image.qcow2")
	if err := os.WriteFile(dummyImagePath, []byte("dummy-disk-image-data"), 0644); err != nil {
		t.Fatalf("failed to create dummy disk image: %v", err)
	}

	// 0. Verify non-existent image rejection
	_, missingErr := rt.Start(context.Background(), vm, "non-existent-image.qcow2")
	if missingErr == nil {
		t.Fatal("expected error when starting VM with non-existent disk image, got nil")
	}

	// 1. Start VM
	pid, err := rt.Start(context.Background(), vm, dummyImagePath)
	if err != nil {
		t.Fatalf("failed to start VM process: %v", err)
	}
	if pid <= 0 {
		t.Fatalf("invalid PID returned: %d", pid)
	}

	// 2. Verify IsAlive
	if !rt.IsAlive(pid) {
		t.Fatalf("expected PID %d to be alive", pid)
	}

	// Wait briefly for boot logs
	time.Sleep(1 * time.Second)

	// 3. Inspect Logs
	logs, err := rt.GetLogs("test-runtime-vm")
	if err != nil {
		t.Fatalf("failed to read logs: %v", err)
	}
	if !strings.Contains(logs, "[BOOT] Starting Virtual Machine: test-runtime-vm") {
		t.Errorf("expected boot logs, got: %s", logs)
	}

	// 4. Stop VM
	if err := rt.Stop(pid); err != nil {
		t.Fatalf("failed to stop VM: %v", err)
	}

	// 5. Verify process is no longer alive
	time.Sleep(500 * time.Millisecond)
	if rt.IsAlive(pid) {
		t.Errorf("expected PID %d to be dead after Stop", pid)
	}
}

func TestProcessVMRuntime_PathTraversalRejection(t *testing.T) {
	tempLogsDir, err := os.MkdirTemp("", "gdc-runtime-logs-traversal-*")
	if err != nil {
		t.Fatalf("failed to create temp logs dir: %v", err)
	}
	defer os.RemoveAll(tempLogsDir)

	absRunnerPath, err := filepath.Abs("../../bin/vm-runner.exe")
	if err != nil {
		t.Fatalf("failed to resolve runner path: %v", err)
	}

	rt, err := NewProcessVMRuntime(tempLogsDir, absRunnerPath)
	if err != nil {
		t.Fatalf("failed to initialize runtime: %v", err)
	}

	traversalVM := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "../../evil-vm"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 256, DiskGB: 10},
		},
	}

	dummyPath := filepath.Join(tempLogsDir, "disk.qcow2")
	_ = os.WriteFile(dummyPath, []byte("data"), 0644)

	if _, err := rt.Start(context.Background(), traversalVM, dummyPath); err == nil {
		t.Fatal("expected error starting VM with traversal name, got nil")
	}

	if _, err := rt.GetLogs("../../evil-vm"); err == nil {
		t.Fatal("expected error getting logs for traversal name, got nil")
	}
}


