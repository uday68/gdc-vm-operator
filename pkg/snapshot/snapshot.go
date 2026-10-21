// Package snapshot coordinates point-in-time file-artifact snapshot backups and restores.
//
// NOTE ON HYPERVISOR BOUNDARIES:
// Snapshots in this package capture an offline file copy of an image artifact on disk.
// This is NOT a crash-consistent, memory-consistent, or application-consistent snapshot of
// an active running hypervisor VM. Workloads should be stopped or paused prior to snapshotting
// to avoid capturing dirty in-flight guest filesystem buffers.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/pathutil"
	"gopkg.in/yaml.v3"
)

// SnapshotMeta represents an offline point-in-time disk and state capture.
type SnapshotMeta struct {
	Name           string                        `json:"name" yaml:"name"`
	VMName         string                        `json:"vmName" yaml:"vmName"`
	Timestamp      time.Time                     `json:"timestamp" yaml:"timestamp"`
	SHA256Checksum string                        `json:"sha256Checksum" yaml:"sha256Checksum"`
	SizeBytes      int64                         `json:"sizeBytes" yaml:"sizeBytes"`
	Resources      v1alpha1.ResourceRequirements `json:"resources" yaml:"resources"`
	SnapshotFile   string                        `json:"snapshotFile" yaml:"snapshotFile"`
}

// Manager coordinates offline point-in-time VM backups.
type Manager struct {
	baseDir string
	mu      sync.RWMutex
}

// NewManager initializes the snapshot storage directory.
func NewManager(baseDir string) (*Manager, error) {
	if strings.TrimSpace(baseDir) == "" {
		return nil, fmt.Errorf("snapshot base directory cannot be empty")
	}

	cleanBase, err := filepath.Abs(filepath.Clean(baseDir))
	if err != nil {
		return nil, fmt.Errorf("invalid snapshot base directory: %w", err)
	}

	if err := os.MkdirAll(cleanBase, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot base directory: %w", err)
	}

	return &Manager{baseDir: cleanBase}, nil
}

// Create captures a point-in-time disk binary copy in a temporary location, computes its
// verification hash, verifies byte count, and atomically publishes both disk and metadata.
// If any stage fails, all temporary files are purged and no partial snapshot is exposed.
func (m *Manager) Create(vm *v1alpha1.VirtualMachine, snapshotName, sourceDiskPath string) (*SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if vm == nil {
		return nil, fmt.Errorf("virtual machine cannot be nil")
	}
	if err := pathutil.ValidateResourceName(vm.Metadata.Name); err != nil {
		return nil, fmt.Errorf("invalid VM name: %w", err)
	}
	if err := pathutil.ValidateResourceName(snapshotName); err != nil {
		return nil, fmt.Errorf("invalid snapshot name: %w", err)
	}

	vmSnapDir, err := pathutil.SafeJoin(m.baseDir, vm.Metadata.Name)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(vmSnapDir, 0755); err != nil {
		return nil, fmt.Errorf("failed creating VM snapshot directory: %w", err)
	}

	metaFile, err := pathutil.SafeJoin(vmSnapDir, fmt.Sprintf("%s.yaml", snapshotName))
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(metaFile); err == nil {
		return nil, fmt.Errorf("snapshot '%s' already exists for VM '%s'", snapshotName, vm.Metadata.Name)
	}

	snapDiskFile, err := pathutil.SafeJoin(vmSnapDir, fmt.Sprintf("%s.snap", snapshotName))
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(snapDiskFile); err == nil {
		return nil, fmt.Errorf("snapshot disk file '%s' already exists for VM '%s'", snapDiskFile, vm.Metadata.Name)
	}

	// 1. Verify source disk exists and capture source metadata
	srcStat, err := os.Stat(sourceDiskPath)
	if err != nil {
		return nil, fmt.Errorf("failed accessing source disk: %w", err)
	}
	if srcStat.IsDir() {
		return nil, fmt.Errorf("source disk '%s' cannot be a directory", sourceDiskPath)
	}

	src, err := os.Open(sourceDiskPath)
	if err != nil {
		return nil, fmt.Errorf("failed opening source disk: %w", err)
	}
	defer src.Close()

	// 2. Stream to temporary disk file first (never directly to final destination)
	tmpDisk, err := os.CreateTemp(vmSnapDir, fmt.Sprintf(".tmp-%s-disk-*.snap", snapshotName))
	if err != nil {
		return nil, fmt.Errorf("failed creating temporary snapshot disk: %w", err)
	}
	tmpDiskPath := tmpDisk.Name()

	var tmpMetaPath string
	var success bool
	defer func() {
		if !success {
			_ = tmpDisk.Close()
			_ = os.Remove(tmpDiskPath)
			if tmpMetaPath != "" {
				_ = os.Remove(tmpMetaPath)
			}
			_ = os.Remove(snapDiskFile)
			_ = os.Remove(metaFile)
		}
	}()

	hasher := sha256.New()
	writer := io.MultiWriter(tmpDisk, hasher)

	copiedBytes, err := io.Copy(writer, src)
	if err != nil {
		return nil, fmt.Errorf("failed during streaming snapshot copy: %w", err)
	}

	// Verify exact byte count matches source file
	if copiedBytes != srcStat.Size() {
		return nil, fmt.Errorf("snapshot copy truncated: expected %d bytes, copied %d bytes", srcStat.Size(), copiedBytes)
	}

	if err := tmpDisk.Sync(); err != nil {
		return nil, fmt.Errorf("failed syncing snapshot disk to storage: %w", err)
	}
	if err := tmpDisk.Close(); err != nil {
		return nil, fmt.Errorf("failed closing snapshot disk file: %w", err)
	}

	hashStr := hex.EncodeToString(hasher.Sum(nil))

	// 3. Write metadata to temporary manifest file
	meta := SnapshotMeta{
		Name:           snapshotName,
		VMName:         vm.Metadata.Name,
		Timestamp:      time.Now().UTC(),
		SHA256Checksum: hashStr,
		SizeBytes:      copiedBytes,
		Resources:      vm.Spec.Resources,
		SnapshotFile:   fmt.Sprintf("%s.snap", snapshotName), // Safe relative filename
	}

	data, err := yaml.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}

	tmpMeta, err := os.CreateTemp(vmSnapDir, fmt.Sprintf(".tmp-%s-meta-*.yaml", snapshotName))
	if err != nil {
		return nil, fmt.Errorf("failed creating temp snapshot metadata: %w", err)
	}
	tmpMetaPath = tmpMeta.Name()

	if _, err := tmpMeta.Write(data); err != nil {
		_ = tmpMeta.Close()
		return nil, fmt.Errorf("failed writing snapshot manifest: %w", err)
	}
	if err := tmpMeta.Sync(); err != nil {
		_ = tmpMeta.Close()
		return nil, fmt.Errorf("failed syncing snapshot manifest: %w", err)
	}
	if err := tmpMeta.Close(); err != nil {
		return nil, fmt.Errorf("failed closing snapshot manifest: %w", err)
	}

	// 4. Atomically publish disk first, then manifest
	if err := safeReplaceFile(tmpDiskPath, snapDiskFile); err != nil {
		return nil, fmt.Errorf("failed publishing snapshot disk: %w", err)
	}

	if err := safeReplaceFile(tmpMetaPath, metaFile); err != nil {
		return nil, fmt.Errorf("failed publishing snapshot manifest: %w", err)
	}

	success = true
	return &meta, nil
}

// Restore recovers a VM disk from a verified point-in-time snapshot.
// It strictly validates metadata identity, recomputes the SHA-256 digest, rejects accidental overwrites
// by default, and writes to a temporary file before atomically committing to the target destination.
func (m *Manager) Restore(vmName, snapshotName, targetDiskPath string, allowOverwrite bool) (*SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := pathutil.ValidateResourceName(vmName); err != nil {
		return nil, fmt.Errorf("invalid VM name: %w", err)
	}
	if err := pathutil.ValidateResourceName(snapshotName); err != nil {
		return nil, fmt.Errorf("invalid snapshot name: %w", err)
	}
	if strings.TrimSpace(targetDiskPath) == "" {
		return nil, fmt.Errorf("target disk path cannot be empty")
	}

	cleanTarget, err := filepath.Abs(filepath.Clean(targetDiskPath))
	if err != nil {
		return nil, fmt.Errorf("invalid target disk path: %w", err)
	}

	// Overwrite guard: default to refusing accidental overwrite
	if _, err := os.Stat(cleanTarget); err == nil && !allowOverwrite {
		return nil, fmt.Errorf("target disk '%s' already exists; set allowOverwrite=true to overwrite", cleanTarget)
	}

	vmSnapDir, err := pathutil.SafeJoin(m.baseDir, vmName)
	if err != nil {
		return nil, err
	}

	metaFile, err := pathutil.SafeJoin(vmSnapDir, fmt.Sprintf("%s.yaml", snapshotName))
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(metaFile)
	if err != nil {
		return nil, fmt.Errorf("snapshot '%s' for VM '%s' not found: %w", snapshotName, vmName, err)
	}

	var meta SnapshotMeta
	if err := yaml.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot manifest: %w", err)
	}

	// Validate metadata integrity against requested identities
	if meta.Name != snapshotName {
		return nil, fmt.Errorf("snapshot metadata corrupted: expected name '%s', got '%s'", snapshotName, meta.Name)
	}
	if meta.VMName != vmName {
		return nil, fmt.Errorf("snapshot metadata corrupted: expected VM name '%s', got '%s'", vmName, meta.VMName)
	}
	if len(meta.SHA256Checksum) != 64 {
		return nil, fmt.Errorf("snapshot metadata corrupted: invalid SHA256 checksum format: '%s'", meta.SHA256Checksum)
	}
	if meta.SizeBytes < 0 {
		return nil, fmt.Errorf("snapshot metadata corrupted: negative size %d", meta.SizeBytes)
	}

	// Security: Do NOT trust arbitrary SnapshotFile paths from metadata YAML.
	// Resolve strictly under the managed snapshot directory.
	snapDiskFile, err := pathutil.SafeJoin(vmSnapDir, fmt.Sprintf("%s.snap", snapshotName))
	if err != nil {
		return nil, fmt.Errorf("unsafe snapshot disk path: %w", err)
	}

	diskStat, err := os.Stat(snapDiskFile)
	if err != nil {
		return nil, fmt.Errorf("snapshot disk file '%s' inaccessible: %w", snapDiskFile, err)
	}
	if diskStat.Size() != meta.SizeBytes {
		return nil, fmt.Errorf("snapshot disk size corrupted: expected %d bytes, got %d bytes", meta.SizeBytes, diskStat.Size())
	}

	src, err := os.Open(snapDiskFile)
	if err != nil {
		return nil, fmt.Errorf("failed opening snapshot disk: %w", err)
	}
	defer src.Close()

	// Verify cryptographic SHA-256 integrity before touching target disk
	hasher := sha256.New()
	if _, err := io.Copy(hasher, src); err != nil {
		return nil, fmt.Errorf("failed hashing snapshot disk: %w", err)
	}
	computedHash := hex.EncodeToString(hasher.Sum(nil))

	if !strings.EqualFold(computedHash, meta.SHA256Checksum) {
		return nil, fmt.Errorf("snapshot disk corrupted: checksum mismatch (expected %s, got %s)", meta.SHA256Checksum, computedHash)
	}

	if _, err := src.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed seeking snapshot disk: %w", err)
	}

	targetDir := filepath.Dir(cleanTarget)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return nil, fmt.Errorf("failed creating target disk directory: %w", err)
	}

	// Restore via temporary file in target directory to ensure atomic publication
	tmpRestore, err := os.CreateTemp(targetDir, ".tmp-restore-*")
	if err != nil {
		return nil, fmt.Errorf("failed creating temp restore file: %w", err)
	}
	tmpRestorePath := tmpRestore.Name()

	var restoreSuccess bool
	defer func() {
		if !restoreSuccess {
			_ = tmpRestore.Close()
			_ = os.Remove(tmpRestorePath)
		}
	}()

	copiedBytes, err := io.Copy(tmpRestore, src)
	if err != nil {
		return nil, fmt.Errorf("failed writing restored disk: %w", err)
	}
	if copiedBytes != meta.SizeBytes {
		return nil, fmt.Errorf("restored disk copy truncated: expected %d bytes, copied %d bytes", meta.SizeBytes, copiedBytes)
	}

	if err := tmpRestore.Sync(); err != nil {
		return nil, fmt.Errorf("failed syncing restored disk: %w", err)
	}
	if err := tmpRestore.Close(); err != nil {
		return nil, fmt.Errorf("failed closing restored disk: %w", err)
	}

	// Overwrite guard re-check before commit
	if _, err := os.Stat(cleanTarget); err == nil && !allowOverwrite {
		return nil, fmt.Errorf("target disk '%s' already exists; refusal to overwrite without allowOverwrite=true", cleanTarget)
	}

	// Atomically replace target
	if err := safeReplaceFile(tmpRestorePath, cleanTarget); err != nil {
		return nil, fmt.Errorf("failed committing restored disk: %w", err)
	}

	restoreSuccess = true
	return &meta, nil
}

// List returns all recorded snapshots for a VM, ignoring temporary files.
func (m *Manager) List(vmName string) ([]SnapshotMeta, error) {
	if err := pathutil.ValidateResourceName(vmName); err != nil {
		return nil, fmt.Errorf("invalid VM name: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	vmSnapDir, err := pathutil.SafeJoin(m.baseDir, vmName)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(vmSnapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed reading snapshot directory: %w", err)
	}

	var results []SnapshotMeta
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".tmp-") || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		manifestPath := filepath.Join(vmSnapDir, entry.Name())
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("failed reading snapshot manifest %s: %w", entry.Name(), err)
		}
		var meta SnapshotMeta
		if err := yaml.Unmarshal(data, &meta); err != nil {
			return nil, fmt.Errorf("corrupted snapshot manifest %s: %w", entry.Name(), err)
		}
		if meta.VMName == vmName {
			results = append(results, meta)
		}
	}

	return results, nil
}

// safeReplaceFile replaces target with temp file, retrying on transient Windows sharing locks.
func safeReplaceFile(tmpPath, targetPath string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		err := os.Rename(tmpPath, targetPath)
		if err == nil {
			return nil
		}
		lastErr = err

		// On Windows, rename fails if destination exists; remove destination and retry
		_ = os.Remove(targetPath)
		err = os.Rename(tmpPath, targetPath)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return lastErr
}
