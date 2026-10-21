package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
)

func TestSnapshotManager_Lifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-snapshot-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := NewManager(tempDir)
	if err != nil {
		t.Fatalf("failed to init manager: %v", err)
	}

	// 1. Create a dummy source disk
	sourceDisk := filepath.Join(tempDir, "original-disk.raw")
	originalPayload := []byte("CRITICAL_DATABASE_STATE_POINT_IN_TIME_VERSION_1")
	if err := os.WriteFile(sourceDisk, originalPayload, 0644); err != nil {
		t.Fatalf("failed to write original disk: %v", err)
	}

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "prod-db"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 4, MemoryMB: 8192, DiskGB: 100},
		},
	}

	// 2. Create Snapshot
	snapMeta, err := mgr.Create(vm, "backup-v1", sourceDisk)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}
	if snapMeta.SHA256Checksum == "" {
		t.Errorf("expected non-empty SHA256 checksum")
	}

	// 3. List Snapshots
	list, err := mgr.List("prod-db")
	if err != nil {
		t.Fatalf("failed to list snapshots: %v", err)
	}
	if len(list) != 1 || list[0].Name != "backup-v1" {
		t.Fatalf("expected 1 snapshot named 'backup-v1', got %v", list)
	}

	// 4. Simulate Disk Corruption / Overwrite
	_ = os.WriteFile(sourceDisk, []byte("CORRUPTED_DISK_DATA_ACCIDENTAL_DROP_TABLE"), 0644)

	// 5. Restore from Snapshot
	restoredPath := filepath.Join(tempDir, "restored-disk.raw")
	restoredMeta, err := mgr.Restore("prod-db", "backup-v1", restoredPath)
	if err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}
	if restoredMeta.Name != "backup-v1" {
		t.Errorf("expected restored meta name backup-v1, got %s", restoredMeta.Name)
	}

	// 6. Verify restored data matches original byte-for-byte
	restoredBytes, err := os.ReadFile(restoredPath)
	if err != nil {
		t.Fatalf("failed to read restored disk: %v", err)
	}
	if string(restoredBytes) != string(originalPayload) {
		t.Errorf("restored payload mismatch: expected '%s', got '%s'", originalPayload, restoredBytes)
	}
}
