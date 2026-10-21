package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// CatalogEntry metadata for an offline image in the air-gapped repository.
type CatalogEntry struct {
	Name        string    `json:"name" yaml:"name"`
	Filename    string    `json:"filename" yaml:"filename"`
	SHA256      string    `json:"sha256" yaml:"sha256"`
	SizeBytes   int64     `json:"sizeBytes" yaml:"sizeBytes"`
	OSFamily    string    `json:"osFamily" yaml:"osFamily"`
	Description string    `json:"description" yaml:"description"`
	AddedAt     time.Time `json:"addedAt" yaml:"addedAt"`
}

// Catalog represents the registry's offline index file.
type Catalog struct {
	Version string                  `yaml:"version"`
	Images  map[string]CatalogEntry `yaml:"images"`
}

// LocalRegistry manages air-gapped image files on local storage.
type LocalRegistry struct {
	baseDir     string
	catalogPath string
	mu          sync.RWMutex
	catalog     Catalog
}

// NewLocalRegistry initializes a registry manager with a base directory.
func NewLocalRegistry(baseDir string) (*LocalRegistry, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create registry base directory: %w", err)
	}

	imagesDir := filepath.Join(baseDir, "images")
	if err := os.MkdirAll(imagesDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create images directory: %w", err)
	}

	catalogPath := filepath.Join(baseDir, "catalog.yaml")
	reg := &LocalRegistry{
		baseDir:     baseDir,
		catalogPath: catalogPath,
		catalog: Catalog{
			Version: "v1",
			Images:  make(map[string]CatalogEntry),
		},
	}

	if err := reg.loadCatalog(); err != nil {
		return nil, err
	}

	return reg, nil
}

func (r *LocalRegistry) loadCatalog() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := os.Stat(r.catalogPath); os.IsNotExist(err) {
		return r.saveCatalogLocked()
	}

	data, err := os.ReadFile(r.catalogPath)
	if err != nil {
		return fmt.Errorf("failed to read catalog file: %w", err)
	}

	if err := yaml.Unmarshal(data, &r.catalog); err != nil {
		return fmt.Errorf("failed to parse catalog: %w", err)
	}
	if r.catalog.Images == nil {
		r.catalog.Images = make(map[string]CatalogEntry)
	}
	return nil
}

func (r *LocalRegistry) saveCatalogLocked() error {
	data, err := yaml.Marshal(r.catalog)
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}
	return os.WriteFile(r.catalogPath, data, 0644)
}

// RegisterImage adds or updates an image in the air-gapped catalog and calculates its SHA256 checksum.
func (r *LocalRegistry) RegisterImage(name, filename, osFamily, description string) (*CatalogEntry, error) {
	filePath := filepath.Join(r.baseDir, "images", filename)
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("image binary %s not found in registry path: %w", filename, err)
	}

	hash, err := ComputeSHA256(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to compute image checksum: %w", err)
	}

	entry := CatalogEntry{
		Name:        name,
		Filename:    filename,
		SHA256:      hash,
		SizeBytes:   info.Size(),
		OSFamily:    osFamily,
		Description: description,
		AddedAt:     time.Now(),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.catalog.Images[name] = entry

	if err := r.saveCatalogLocked(); err != nil {
		return nil, err
	}

	return &entry, nil
}

// GetImage retrieves an image entry by name.
func (r *LocalRegistry) GetImage(name string) (*CatalogEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.catalog.Images[name]
	if !ok {
		return nil, false
	}
	return &entry, true
}

// ListImages returns all registered offline images.
func (r *LocalRegistry) ListImages() []CatalogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]CatalogEntry, 0, len(r.catalog.Images))
	for _, entry := range r.catalog.Images {
		list = append(list, entry)
	}
	return list
}

// GetImagePath returns the full path on disk to the image.
func (r *LocalRegistry) GetImagePath(filename string) string {
	return filepath.Join(r.baseDir, "images", filename)
}

// ComputeSHA256 calculates the SHA256 cryptographic hash of a file.
func ComputeSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

