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
	Name           string                         `json:"name" yaml:"name"`
	VMName         string                         `json:"vmName" yaml:"vmName"`
	Timestamp      time.Time                      `json:"timestamp" yaml:"timestamp"`
	SHA256Checksum string                         `json:"sha256Checksum" yaml:"sha256Checksum"`
	SizeBytes      int64                          `json:"sizeBytes" yaml:"sizeBytes"`
	Resources      v1alpha1.ResourceRequirements `json:"resources" yaml:"resources"`
	SnapshotFile   string                         `json:"snapshotFile" yaml:"snapshotFile"`
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

// Create captures a point-in-time disk binary copy and computes its verification hash.
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

	// 1. Open source disk
	src, err := os.Open(sourceDiskPath)
	if err != nil {
		return nil, fmt.Errorf("failed opening source disk: %w", err)
	}
	defer src.Close()

	// 2. Stream to destination disk with SHA256 computation
	dst, err := os.Create(snapDiskFile)
	if err != nil {
		return nil, fmt.Errorf("failed creating snapshot disk destination: %w", err)
	}

	var writeSuccess bool
	defer func() {
		if !writeSuccess {
			_ = dst.Close()
			_ = os.Remove(snapDiskFile)
			_ = os.Remove(metaFile)
		}
	}()

	hasher := sha256.New()
	writer := io.MultiWriter(dst, hasher)

	copiedBytes, err := io.Copy(writer, src)
	if err != nil {
		return nil, fmt.Errorf("failed during streaming snapshot copy: %w", err)
	}

	if err := dst.Sync(); err != nil {
		return nil, fmt.Errorf("failed syncing snapshot disk to storage: %w", err)
	}

	if err := dst.Close(); err != nil {
		return nil, fmt.Errorf("failed closing snapshot disk file: %w", err)
	}

	hashStr := hex.EncodeToString(hasher.Sum(nil))

	meta := SnapshotMeta{
		Name:           snapshotName,
		VMName:         vm.Metadata.Name,
		Timestamp:      time.Now().UTC(),
		SHA256Checksum: hashStr,
		SizeBytes:      copiedBytes,
		Resources:      vm.Spec.Resources,
		SnapshotFile:   snapDiskFile,
	}

	data, err := yaml.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}

	// 3. Write manifest atomically
	tmpMeta, err := os.CreateTemp(vmSnapDir, fmt.Sprintf(".tmp-%s-*.yaml", snapshotName))
	if err != nil {
		return nil, fmt.Errorf("failed creating temp snapshot metadata: %w", err)
	}
	tmpMetaPath := tmpMeta.Name()

	if _, err := tmpMeta.Write(data); err != nil {
		_ = tmpMeta.Close()
		_ = os.Remove(tmpMetaPath)
		return nil, fmt.Errorf("failed writing snapshot manifest: %w", err)
	}
	if err := tmpMeta.Sync(); err != nil {
		_ = tmpMeta.Close()
		_ = os.Remove(tmpMetaPath)
		return nil, fmt.Errorf("failed syncing snapshot manifest: %w", err)
	}
	if err := tmpMeta.Close(); err != nil {
		_ = os.Remove(tmpMetaPath)
		return nil, fmt.Errorf("failed closing snapshot manifest: %w", err)
	}

	if err := os.Rename(tmpMetaPath, metaFile); err != nil {
		_ = os.Remove(tmpMetaPath)
		return nil, fmt.Errorf("failed committing snapshot manifest: %w", err)
	}

	writeSuccess = true
	return &meta, nil
}

// Restore recovers a VM disk from a verified point-in-time snapshot.
func (m *Manager) Restore(vmName, snapshotName, targetDiskPath string) (*SnapshotMeta, error) {
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

	metaFile, err := pathutil.SafeJoin(m.baseDir, filepath.Join(vmName, fmt.Sprintf("%s.yaml", snapshotName)))
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

	// Verify cryptographic integrity of the snapshot disk before restoring
	src, err := os.Open(meta.SnapshotFile)
	if err != nil {
		return nil, fmt.Errorf("failed opening snapshot disk: %w", err)
	}
	defer src.Close()

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

	// Restore via temporary file to ensure atomic disk write
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

	if _, err := io.Copy(tmpRestore, src); err != nil {
		return nil, fmt.Errorf("failed writing restored disk: %w", err)
	}
	if err := tmpRestore.Sync(); err != nil {
		return nil, fmt.Errorf("failed syncing restored disk: %w", err)
	}
	if err := tmpRestore.Close(); err != nil {
		return nil, fmt.Errorf("failed closing restored disk: %w", err)
	}

	// Atomically replace target
	if err := os.Rename(tmpRestorePath, cleanTarget); err != nil {
		return nil, fmt.Errorf("failed committing restored disk: %w", err)
	}

	restoreSuccess = true
	return &meta, nil
}

// List returns all recorded snapshots for a VM.
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
		results = append(results, meta)
	}

	return results, nil
}
