package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
)

func TestFileStore_CRUDAndRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-store-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	s, err := NewFileStore(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}

	vm := &v1alpha1.VirtualMachine{
		APIVersion: "gdc.infrastructure.io/v1alpha1",
		Kind:       "VirtualMachine",
		Metadata: v1alpha1.ObjectMeta{
			Name:      "test-vm-01",
			CreatedAt: time.Now(),
		},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 4096, DiskGB: 20},
			Image:     v1alpha1.ImageSource{Name: "ubuntu-22.04"},
		},
		Status: v1alpha1.VirtualMachineStatus{
			Phase: v1alpha1.PhasePending,
		},
	}

	// 1. Save
	if err := s.Save(vm); err != nil {
		t.Fatalf("failed to save VM: %v", err)
	}

	// 2. Get
	fetched, err := s.Get("test-vm-01")
	if err != nil {
		t.Fatalf("failed to get VM: %v", err)
	}
	if fetched.Spec.Resources.VCPU != 2 {
		t.Errorf("expected 2 vCPUs, got %d", fetched.Spec.Resources.VCPU)
	}

	// 3. Verify on-disk file was created
	stateFile := filepath.Join(tempDir, "test-vm-01.yaml")
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Fatalf("state file was not created on disk: %s", stateFile)
	}

	// 4. Test Crash Recovery: Initialize a new store on the same directory
	recoveredStore, err := NewFileStore(tempDir)
	if err != nil {
		t.Fatalf("failed to reload store from disk: %v", err)
	}
	recoveredVM, err := recoveredStore.Get("test-vm-01")
	if err != nil {
		t.Fatalf("failed to recover VM from reloaded store: %v", err)
	}
	if recoveredVM.Metadata.Name != "test-vm-01" {
		t.Errorf("expected 'test-vm-01', got '%s'", recoveredVM.Metadata.Name)
	}

	// 5. Delete
	if err := s.Delete("test-vm-01"); err != nil {
		t.Fatalf("failed to delete VM: %v", err)
	}
	if _, err := s.Get("test-vm-01"); err == nil {
		t.Fatalf("expected error getting deleted VM, got nil")
	}
}

func TestFileStore_ConcurrentAccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-store-concurrent-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	s, err := NewFileStore(tempDir)
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}

	var wg sync.WaitGroup
	workers := 10

	for i := 0; i < workers; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			vmName := "worker-vm"
			vm := &v1alpha1.VirtualMachine{
				Metadata: v1alpha1.ObjectMeta{Name: vmName},
				Spec:     v1alpha1.VirtualMachineSpec{Resources: v1alpha1.ResourceRequirements{VCPU: idx + 1}},
			}
			_ = s.Save(vm)
			_, _ = s.Get(vmName)
			_ = s.List()
		}()
	}

	wg.Wait()
}

