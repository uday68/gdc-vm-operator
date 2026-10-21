package snapshot

import (
	"os"
	"path/filepath"
	"strings"
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
	restoredMeta, err := mgr.Restore("prod-db", "backup-v1", restoredPath, false)
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

func TestSnapshotManager_PathTraversalRejection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-snapshot-traversal-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := NewManager(tempDir)
	if err != nil {
		t.Fatalf("failed to init manager: %v", err)
	}

	dummyDisk := filepath.Join(tempDir, "disk.raw")
	_ = os.WriteFile(dummyDisk, []byte("dummy"), 0644)

	traversalVM := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "../../escape-vm"},
	}

	// Traversal in VM name
	if _, err := mgr.Create(traversalVM, "snap-1", dummyDisk); err == nil {
		t.Fatalf("expected error creating snapshot with traversal VM name, got nil")
	}

	// Traversal in snapshot name
	validVM := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "valid-vm"},
	}
	if _, err := mgr.Create(validVM, "../../evil-snap", dummyDisk); err == nil {
		t.Fatalf("expected error creating snapshot with traversal snapshot name, got nil")
	}

	// Traversal in Restore
	if _, err := mgr.Restore("../../escape-vm", "snap-1", dummyDisk, false); err == nil {
		t.Fatalf("expected error restoring traversal VM name, got nil")
	}
	if _, err := mgr.Restore("valid-vm", "../../evil-snap", dummyDisk, false); err == nil {
		t.Fatalf("expected error restoring traversal snapshot name, got nil")
	}

	// Traversal in List
	if _, err := mgr.List("../../escape-vm"); err == nil {
		t.Fatalf("expected error listing traversal VM name, got nil")
	}
}

func TestSnapshotManager_OverwriteSemantics(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-snapshot-overwrite-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := NewManager(tempDir)
	if err != nil {
		t.Fatalf("failed to init manager: %v", err)
	}

	sourceDisk := filepath.Join(tempDir, "source.raw")
	_ = os.WriteFile(sourceDisk, []byte("SNAPSHOT_ORIGINAL_DATA"), 0644)

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "vm-test"},
		Spec:     v1alpha1.VirtualMachineSpec{Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 512, DiskGB: 10}},
	}

	// 1. Initial snapshot create
	_, err = mgr.Create(vm, "snap-v1", sourceDisk)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// 2. Duplicate snapshot name must be rejected
	_, err = mgr.Create(vm, "snap-v1", sourceDisk)
	if err == nil {
		t.Fatalf("expected duplicate snapshot creation to be rejected, but succeeded")
	}

	// 3. Restore to existing destination WITHOUT allowOverwrite must be rejected
	targetDisk := filepath.Join(tempDir, "existing-target.raw")
	_ = os.WriteFile(targetDisk, []byte("PREEXISTING_FILE_DO_NOT_CLOBBER"), 0644)

	_, err = mgr.Restore("vm-test", "snap-v1", targetDisk, false)
	if err == nil {
		t.Fatalf("expected restore to existing target without overwrite to fail, but succeeded")
	}

	// Verify target was not modified or truncated
	existingContent, _ := os.ReadFile(targetDisk)
	if string(existingContent) != "PREEXISTING_FILE_DO_NOT_CLOBBER" {
		t.Fatalf("existing file was clobbered or truncated on rejected restore: %s", string(existingContent))
	}

	// 4. Restore WITH allowOverwrite=true must succeed and replace destination
	_, err = mgr.Restore("vm-test", "snap-v1", targetDisk, true)
	if err != nil {
		t.Fatalf("expected restore with allowOverwrite=true to succeed, got: %v", err)
	}
	overwrittenContent, _ := os.ReadFile(targetDisk)
	if string(overwrittenContent) != "SNAPSHOT_ORIGINAL_DATA" {
		t.Fatalf("expected overwritten content to match snapshot, got: %s", string(overwrittenContent))
	}
}

func TestSnapshotManager_IntegrityAndTamperDetection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-snapshot-integrity-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := NewManager(tempDir)
	if err != nil {
		t.Fatalf("failed to init manager: %v", err)
	}

	sourceDisk := filepath.Join(tempDir, "source.raw")
	_ = os.WriteFile(sourceDisk, []byte("INTEGRITY_CHECK_PAYLOAD"), 0644)

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "secure-vm"},
		Spec:     v1alpha1.VirtualMachineSpec{Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 512, DiskGB: 10}},
	}

	meta, err := mgr.Create(vm, "sec-snap-1", sourceDisk)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	snapDir := filepath.Join(tempDir, "secure-vm")
	diskFile := filepath.Join(snapDir, "sec-snap-1.snap")
	yamlFile := filepath.Join(snapDir, "sec-snap-1.yaml")

	// 1. Bit-flip tampering in disk file: corrupt 1 byte
	_ = os.WriteFile(diskFile, []byte("INTEGRITY_CHECK_PAYL0AD"), 0644) // 'O' -> '0'
	targetPath := filepath.Join(tempDir, "restored.raw")
	_, err = mgr.Restore("secure-vm", "sec-snap-1", targetPath, false)
	if err == nil {
		t.Fatalf("expected tampered snapshot disk to be rejected by SHA-256 verification")
	}

	// Restore original disk
	_ = os.WriteFile(diskFile, []byte("INTEGRITY_CHECK_PAYLOAD"), 0644)

	// 2. Tamper metadata: modify SnapshotFile to point to external file (path injection attempt)
	meta.SnapshotFile = "/etc/shadow"
	metaBytes, _ := os.ReadFile(yamlFile)
	tamperedYAML := strings.Replace(string(metaBytes), "sec-snap-1.snap", "/etc/shadow", 1)
	_ = os.WriteFile(yamlFile, []byte(tamperedYAML), 0644)

	// Restore must still strictly resolve sec-snap-1.snap locally and succeed without following /etc/shadow
	_, err = mgr.Restore("secure-vm", "sec-snap-1", targetPath, true)
	if err != nil {
		t.Fatalf("expected restore to safely ignore arbitrary SnapshotFile path from metadata: %v", err)
	}

	// 3. Truncated/corrupted metadata YAML
	_ = os.WriteFile(yamlFile, []byte("NOT_A_VALID_YAML: [unterminated"), 0644)
	_, err = mgr.Restore("secure-vm", "sec-snap-1", targetPath, true)
	if err == nil {
		t.Fatalf("expected corrupted metadata YAML to be rejected")
	}
}

func TestSnapshotManager_FailedCreateLeavesNoArtifacts(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-snapshot-failed-create-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := NewManager(tempDir)
	if err != nil {
		t.Fatalf("failed to init manager: %v", err)
	}

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "fail-vm"},
		Spec:     v1alpha1.VirtualMachineSpec{Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 512, DiskGB: 10}},
	}

	// Missing source disk
	_, err = mgr.Create(vm, "missing-snap", filepath.Join(tempDir, "non-existent-source.raw"))
	if err == nil {
		t.Fatalf("expected error creating snapshot from non-existent disk")
	}

	// Assert no published files or temp files remain in VM directory
	vmDir := filepath.Join(tempDir, "fail-vm")
	entries, _ := os.ReadDir(vmDir)
	if len(entries) != 0 {
		t.Fatalf("expected zero files in VM snapshot directory after failed create, found %d", len(entries))
	}
}

func BenchmarkSnapshot_SyntheticLargeArtifact(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "gdc-snapshot-bench-*")
	if err != nil {
		b.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := NewManager(tempDir)
	if err != nil {
		b.Fatalf("failed creating manager: %v", err)
	}

	// 5 MB synthetic disk payload
	const payloadSize = 5 * 1024 * 1024
	syntheticData := make([]byte, payloadSize)
	for i := range syntheticData {
		syntheticData[i] = byte(i % 256)
	}

	sourceDisk := filepath.Join(tempDir, "large-source.raw")
	if err := os.WriteFile(sourceDisk, syntheticData, 0644); err != nil {
		b.Fatalf("failed creating synthetic disk: %v", err)
	}

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "bench-vm"},
		Spec:     v1alpha1.VirtualMachineSpec{Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 20}},
	}

	targetRestore := filepath.Join(tempDir, "large-restored.raw")

	b.ResetTimer()
	b.SetBytes(payloadSize)

	for i := 0; i < b.N; i++ {
		snapName := "snap-bench"
		_, err := mgr.Create(vm, snapName, sourceDisk)
		if err != nil {
			b.Fatalf("failed creating snapshot: %v", err)
		}

		_, err = mgr.Restore("bench-vm", snapName, targetRestore, true)
		if err != nil {
			b.Fatalf("failed restoring snapshot: %v", err)
		}

		// Cleanup for next iteration
		_ = os.Remove(targetRestore)
		vmDir := filepath.Join(tempDir, "bench-vm")
		_ = os.RemoveAll(vmDir)
	}
}

