package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/pathutil"
	"gopkg.in/yaml.v3"
)

// Store provides thread-safe persistent storage for VirtualMachine objects.
type Store interface {
	Save(vm *v1alpha1.VirtualMachine) error
	Get(name string) (*v1alpha1.VirtualMachine, error)
	List() []*v1alpha1.VirtualMachine
	Delete(name string) error
}

// FileStore implements Store with an in-memory cache and atomic local disk persistence.
type FileStore struct {
	baseDir string
	mu      sync.RWMutex
	cache   map[string]*v1alpha1.VirtualMachine
}

// NewFileStore initializes the store at the given directory.
func NewFileStore(baseDir string) (*FileStore, error) {
	if strings.TrimSpace(baseDir) == "" {
		return nil, fmt.Errorf("base directory cannot be empty")
	}

	cleanBase, err := filepath.Abs(filepath.Clean(baseDir))
	if err != nil {
		return nil, fmt.Errorf("invalid store path: %w", err)
	}

	if err := os.MkdirAll(cleanBase, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %w", err)
	}

	fs := &FileStore{
		baseDir: cleanBase,
		cache:   make(map[string]*v1alpha1.VirtualMachine),
	}

	if err := fs.recoverFromDisk(); err != nil {
		return nil, fmt.Errorf("failed to recover store from disk: %w", err)
	}

	return fs, nil
}

// recoverFromDisk populates the in-memory cache from persisted YAML files on startup.
func (s *FileStore) recoverFromDisk() error {
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		// Ignore directories, temp files, and non-yaml files
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".tmp-") || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}

		nameWithoutExt := strings.TrimSuffix(entry.Name(), ".yaml")
		if err := pathutil.ValidateResourceName(nameWithoutExt); err != nil {
			// Skip files with invalid or dangerous resource names
			continue
		}

		filePath := filepath.Join(s.baseDir, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read state file %s: %w", filePath, err)
		}

		var vm v1alpha1.VirtualMachine
		if err := yaml.Unmarshal(data, &vm); err != nil {
			return fmt.Errorf("failed to parse state file %s: %w", filePath, err)
		}

		if vm.Metadata.Name != nameWithoutExt {
			return fmt.Errorf("manifest metadata.name '%s' does not match filename '%s'", vm.Metadata.Name, entry.Name())
		}

		s.cache[vm.Metadata.Name] = vm.DeepCopy()
	}

	return nil
}

// Save persists the VM spec and status atomically to disk and updates in-memory cache.
func (s *FileStore) Save(vm *v1alpha1.VirtualMachine) error {
	if vm == nil {
		return fmt.Errorf("cannot save nil virtual machine")
	}
	if err := pathutil.ValidateResourceName(vm.Metadata.Name); err != nil {
		return fmt.Errorf("invalid VM metadata.name: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cloned := vm.DeepCopy()

	targetPath, err := pathutil.SafeJoin(s.baseDir, fmt.Sprintf("%s.yaml", vm.Metadata.Name))
	if err != nil {
		return fmt.Errorf("unsafe target path: %w", err)
	}

	data, err := yaml.Marshal(cloned)
	if err != nil {
		return fmt.Errorf("failed to marshal VM manifest: %w", err)
	}

	// 1. Write to unique temporary file in base directory
	tmpFile, err := os.CreateTemp(s.baseDir, fmt.Sprintf(".tmp-%s-*.yaml", vm.Metadata.Name))
	if err != nil {
		return fmt.Errorf("failed creating temporary state file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Ensure temp file is cleaned up if rename/write fails
	var success bool
	defer func() {
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed writing state to temp file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed syncing state to disk: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed closing temp state file: %w", err)
	}

	// 2. Safe atomic replacement with retry (handles platform sharing locks)
	if err := safeReplaceFile(tmpPath, targetPath); err != nil {
		return fmt.Errorf("failed committing state file atomically: %w", err)
	}

	success = true

	// 3. Update cache only after successful disk commit
	s.cache[vm.Metadata.Name] = cloned
	return nil
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
		// Brief exponential backoff for transient locks (antivirus, indexer)
		time.Sleep(time.Duration(10*(attempt+1)) * time.Millisecond)
	}
	return lastErr
}

// Get retrieves an independent clone of the VM object by name.
func (s *FileStore) Get(name string) (*v1alpha1.VirtualMachine, error) {
	if err := pathutil.ValidateResourceName(name); err != nil {
		return nil, fmt.Errorf("invalid VM name '%s': %w", name, err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	vm, exists := s.cache[name]
	if !exists {
		return nil, fmt.Errorf("virtual machine '%s' not found", name)
	}

	return vm.DeepCopy(), nil
}

// List returns clones of all VMs currently registered in the store.
func (s *FileStore) List() []*v1alpha1.VirtualMachine {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*v1alpha1.VirtualMachine, 0, len(s.cache))
	for _, vm := range s.cache {
		result = append(result, vm.DeepCopy())
	}
	return result
}

// Delete removes the VM from both disk and in-memory cache.
func (s *FileStore) Delete(name string) error {
	if err := pathutil.ValidateResourceName(name); err != nil {
		return fmt.Errorf("invalid VM name '%s': %w", name, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	targetPath, err := pathutil.SafeJoin(s.baseDir, fmt.Sprintf("%s.yaml", name))
	if err != nil {
		return fmt.Errorf("unsafe target path: %w", err)
	}

	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete state file: %w", err)
	}

	delete(s.cache, name)
	return nil
}
