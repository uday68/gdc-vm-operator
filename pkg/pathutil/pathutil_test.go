package pathutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateResourceName(t *testing.T) {
	valid := []string{
		"ubuntu-vm",
		"db-01",
		"a",
		"vm-with-many-hyphens-123",
		"ubuntu-22.04",
		"cirros-0.5.2",
		"node-1.subdomain.example",
	}
	for _, name := range valid {
		if err := ValidateResourceName(name); err != nil {
			t.Errorf("expected '%s' to be valid, got: %v", name, err)
		}
	}

	invalid := []struct {
		name string
		desc string
	}{
		{"", "empty"},
		{"../evil", "relative traversal"},
		{"..\\evil", "windows backslash traversal"},
		{"/root/bad", "absolute path"},
		{"CON", "windows reserved device CON"},
		{"con.yaml", "windows reserved device con with extension"},
		{"nul", "windows reserved device nul"},
		{"nul.tar.gz", "windows reserved device nul with extension"},
		{"com1", "windows reserved device com1"},
		{"VM_NAME", "uppercase not allowed in RFC 1123"},
		{"-start-hyphen", "leading hyphen"},
		{"end-hyphen-", "trailing hyphen"},
		{".start-dot", "leading dot"},
		{"end-dot.", "trailing dot"},
		{"has..double-dot", "consecutive dots"},
		{"..", "parent directory dot dot"},
		{".", "single dot"},
		{"has:colon", "colons not allowed"},
		{"has/slash", "forward slash not allowed"},
		{"has\\backslash", "backslash not allowed"},
	}

	for _, tc := range invalid {
		if err := ValidateResourceName(tc.name); err == nil {
			t.Errorf("expected '%s' (%s) to be rejected, but passed", tc.name, tc.desc)
		}
	}
}

func TestValidateFilename(t *testing.T) {
	valid := []string{
		"ubuntu-22.04.qcow2",
		"cirros-0.5.2.img",
		"disk.raw",
	}
	for _, fn := range valid {
		if err := ValidateFilename(fn); err != nil {
			t.Errorf("expected '%s' to be valid, got: %v", fn, err)
		}
	}

	invalid := []string{
		"",
		"../escape.qcow2",
		"..\\escape.qcow2",
		"/etc/passwd",
		"C:\\Windows\\win.ini",
		"nul.qcow2",
		"con.raw",
		".",
		"..",
		"evil\x00.img",
		"dir/file.img",
	}
	for _, fn := range invalid {
		if err := ValidateFilename(fn); err == nil {
			t.Errorf("expected '%s' to be rejected, but passed", fn)
		}
	}
}

func TestSafeJoin(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pathutil-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Valid join
	joined, err := SafeJoin(tempDir, "safe-file.yaml")
	if err != nil {
		t.Fatalf("unexpected error on safe join: %v", err)
	}
	expected := filepath.Join(tempDir, "safe-file.yaml")
	if joined != expected {
		t.Errorf("expected %s, got %s", expected, joined)
	}

	// Traversal attempt
	_, err = SafeJoin(tempDir, "../../outside.yaml")
	if err == nil {
		t.Fatalf("expected error on path traversal, got nil")
	}

	// Absolute path attempt to escape
	_, err = SafeJoin(tempDir, filepath.Dir(tempDir))
	if err == nil {
		t.Fatalf("expected error when escaping baseDir, got nil")
	}
}
