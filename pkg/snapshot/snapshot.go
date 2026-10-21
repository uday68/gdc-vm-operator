package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
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
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot base directory: %w", err)
	}
	return &Manager{baseDir: baseDir}, nil
}

// Create captures a point-in-time disk binary copy and computes its verification hash.
func (m *Manager) Create(vm *v1alpha1.VirtualMachine, snapshotName, sourceDiskPath string) (*SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if vm == nil || vm.Metadata.Name == "" {
		return nil, fmt.Errorf("invalid virtual machine specification")
	}

	vmSnapDir := filepath.Join(m.baseDir, vm.Metadata.Name)
	if err := os.MkdirAll(vmSnapDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create VM snapshot directory: %w", err)
	}

	metaFile := filepath.Join(vmSnapDir, fmt.Sprintf("%s.yaml", snapshotName))
	if _, err := os.Stat(metaFile); err == nil {
		return nil, fmt.Errorf("snapshot '%s' already exists for VM '%s'", snapshotName, vm.Metadata.Name)
	}

	snapDiskFile := filepath.Join(vmSnapDir, fmt.Sprintf("%s.snap", snapshotName))

	// Copy disk file with streaming hash computation
	src, err := os.Open(sourceDiskPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source disk: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(snapDiskFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot disk destination: %w", err)
	}
	defer dst.Close()

	hasher := sha256.New()
	writer := io.MultiWriter(dst, hasher)

	copiedBytes, err := io.Copy(writer, src)
	if err != nil {
		_ = os.Remove(snapDiskFile)
		return nil, fmt.Errorf("failed during streaming snapshot copy: %w", err)
	}

	hashStr := hex.EncodeToString(hasher.Sum(nil))

	meta := SnapshotMeta{
		Name:           snapshotName,
		VMName:         vm.Metadata.Name,
		Timestamp:      time.Now(),
		SHA256Checksum: hashStr,
		SizeBytes:      copiedBytes,
		Resources:      vm.Spec.Resources,
		SnapshotFile:   snapDiskFile,
	}

	data, err := yaml.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}

	if err := os.WriteFile(metaFile, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to save snapshot manifest: %w", err)
	}

	return &meta, nil
}

// Restore recovers a VM disk from a verified point-in-time snapshot.
func (m *Manager) Restore(vmName, snapshotName, targetDiskPath string) (*SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	metaFile := filepath.Join(m.baseDir, vmName, fmt.Sprintf("%s.yaml", snapshotName))
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
		return nil, fmt.Errorf("failed to open snapshot disk: %w", err)
	}
	defer src.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, src); err != nil {
		return nil, fmt.Errorf("failed to hash snapshot disk: %w", err)
	}
	computedHash := hex.EncodeToString(hasher.Sum(nil))

	if computedHash != meta.SHA256Checksum {
		return nil, fmt.Errorf("snapshot disk corrupted: checksum mismatch (expected %s, got %s)", meta.SHA256Checksum, computedHash)
	}

	// Seek back to start and copy to destination
	if _, err := src.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek disk: %w", err)
	}

	dst, err := os.Create(targetDiskPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open restore destination: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return nil, fmt.Errorf("failed writing restored disk: %w", err)
	}

	return &meta, nil
}

// List returns all recorded snapshots for a VM.
func (m *Manager) List(vmName string) ([]SnapshotMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	vmSnapDir := filepath.Join(m.baseDir, vmName)
	entries, err := os.ReadDir(vmSnapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var results []SnapshotMeta
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(vmSnapDir, entry.Name()))
		if err != nil {
			continue
		}
		var meta SnapshotMeta
		if err := yaml.Unmarshal(data, &meta); err == nil {
			results = append(results, meta)
		}
	}

	return results, nil
}

