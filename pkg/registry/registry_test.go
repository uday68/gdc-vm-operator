package registry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalRegistry_Lifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-registry-test-*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	reg, err := NewLocalRegistry(tempDir)
	if err != nil {
		t.Fatalf("failed initializing registry: %v", err)
	}

	imagesDir := filepath.Join(tempDir, "images")
	testFile := filepath.Join(imagesDir, "cirros-test.img")
	payload := []byte("CIRROS_MINIMAL_TEST_PAYLOAD")
	if err := os.WriteFile(testFile, payload, 0644); err != nil {
		t.Fatalf("failed writing test image: %v", err)
	}

	// 1. Register image
	entry, err := reg.RegisterImage("cirros-test", "cirros-test.img", "linux", "CirrOS Cloud Image")
	if err != nil {
		t.Fatalf("failed registering image: %v", err)
	}
	if entry.SHA256 == "" {
		t.Errorf("expected non-empty SHA256 checksum")
	}

	// 2. GetImage
	got, found := reg.GetImage("cirros-test")
	if !found || got.Name != "cirros-test" {
		t.Errorf("failed retrieving registered image")
	}

	// 3. GetImagePath
	path, err := reg.GetImagePath("cirros-test.img")
	if err != nil {
		t.Fatalf("failed getting image path: %v", err)
	}
	if path != testFile {
		t.Errorf("expected path %s, got %s", testFile, path)
	}

	// 4. Persistence / Reload test
	reloaded, err := NewLocalRegistry(tempDir)
	if err != nil {
		t.Fatalf("failed reloading registry: %v", err)
	}
	if _, found := reloaded.GetImage("cirros-test"); !found {
		t.Errorf("image not found after reload from catalog.yaml")
	}
}

func TestLocalRegistry_PathTraversalRejection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gdc-registry-traversal-*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	reg, err := NewLocalRegistry(tempDir)
	if err != nil {
		t.Fatalf("failed initializing registry: %v", err)
	}

	// Traversal in name
	if _, err := reg.RegisterImage("../../evil-name", "image.qcow2", "linux", ""); err == nil {
		t.Errorf("expected error registering traversal name, got nil")
	}

	// Traversal in filename
	if _, err := reg.RegisterImage("valid-name", "../../../evil.qcow2", "linux", ""); err == nil {
		t.Errorf("expected error registering traversal filename, got nil")
	}

	// Traversal in GetImagePath
	if _, err := reg.GetImagePath("../../evil.qcow2"); err == nil {
		t.Errorf("expected error getting path for traversal filename, got nil")
	}
}

