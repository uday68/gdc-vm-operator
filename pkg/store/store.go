package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
)

// Store provides thread-safe persistent storage for VirtualMachine objects.
type Store interface {
	Save(vm *v1alpha1.VirtualMachine) error
	Get(name string) (*v1alpha1.VirtualMachine, error)
	List() []*v1alpha1.VirtualMachine
	Delete(name string) error
}

// FileStore implements Store with an in-memory cache and local disk persistence.
type FileStore struct {
	baseDir string
	mu      sync.RWMutex
	cache   map[string]*v1alpha1.VirtualMachine
}

// NewFileStore initializes the store at the given directory.
func NewFileStore(baseDir string) (*FileStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %w", err)
	}

	fs := &FileStore{
		baseDir: baseDir,
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
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
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

		s.cache[vm.Metadata.Name] = &vm
	}

	return nil
}

// Save persists the VM spec and status both to memory and disk.
func (s *FileStore) Save(vm *v1alpha1.VirtualMachine) error {
	if vm == nil || vm.Metadata.Name == "" {
		return fmt.Errorf("cannot save VM with empty metadata name")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Clone to prevent external mutation races
	cloned, err := deepCopyVM(vm)
	if err != nil {
		return fmt.Errorf("failed to deep copy VM: %w", err)
	}

	// Persist to disk
	filePath := filepath.Join(s.baseDir, fmt.Sprintf("%s.yaml", vm.Metadata.Name))
	data, err := yaml.Marshal(cloned)
	if err != nil {
		return fmt.Errorf("failed to marshal VM manifest: %w", err)
	}

	// Atomic write via temp file
	tempFile := fmt.Sprintf("%s.tmp", filePath)
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}
	if err := os.Rename(tempFile, filePath); err != nil {
		return fmt.Errorf("failed to commit state file: %w", err)
	}

	s.cache[vm.Metadata.Name] = cloned
	return nil
}

// Get retrieves a clone of the VM object by name.
func (s *FileStore) Get(name string) (*v1alpha1.VirtualMachine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vm, exists := s.cache[name]
	if !exists {
		return nil, fmt.Errorf("virtual machine '%s' not found", name)
	}

	return deepCopyVM(vm)
}

// List returns clones of all VMs currently registered in the store.
func (s *FileStore) List() []*v1alpha1.VirtualMachine {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*v1alpha1.VirtualMachine, 0, len(s.cache))
	for _, vm := range s.cache {
		cloned, err := deepCopyVM(vm)
		if err == nil {
			result = append(result, cloned)
		}
	}
	return result
}

// Delete removes the VM from both in-memory cache and disk.
func (s *FileStore) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := filepath.Join(s.baseDir, fmt.Sprintf("%s.yaml", name))
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete state file: %w", err)
	}

	delete(s.cache, name)
	return nil
}

// deepCopyVM creates an independent copy via YAML round-trip serialization.
func deepCopyVM(orig *v1alpha1.VirtualMachine) (*v1alpha1.VirtualMachine, error) {
	data, err := yaml.Marshal(orig)
	if err != nil {
		return nil, err
	}
	var clone v1alpha1.VirtualMachine
	if err := yaml.Unmarshal(data, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
}

