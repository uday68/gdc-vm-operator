package validator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/registry"
)

func setupTestRegistry(t *testing.T) (*registry.LocalRegistry, string) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "gdc-reg-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	reg, err := registry.NewLocalRegistry(tempDir)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}

	// Create a dummy image file
	dummyImagePath := filepath.Join(tempDir, "images", "ubuntu-22.04.qcow2")
	dummyContent := []byte("dummy-qcow2-image-payload-for-testing")
	if err := os.WriteFile(dummyImagePath, dummyContent, 0644); err != nil {
		t.Fatalf("failed to create dummy image: %v", err)
	}

	// Register it
	entry, err := reg.RegisterImage("ubuntu-22.04", "ubuntu-22.04.qcow2", "linux", "Ubuntu Server 22.04 LTS")
	if err != nil {
		t.Fatalf("failed to register image: %v", err)
	}

	return reg, entry.SHA256
}

func TestValidator_Validate(t *testing.T) {
	reg, validHash := setupTestRegistry(t)
	v := NewValidator(reg, nil)

	tests := []struct {
		name        string
		vm          *v1alpha1.VirtualMachine
		expectError bool
		errorType   string // "airgap" or "validation"
	}{
		{
			name: "Valid air-gapped VM spec",
			vm: &v1alpha1.VirtualMachine{
				Metadata: v1alpha1.ObjectMeta{Name: "prod-database-01"},
				Spec: v1alpha1.VirtualMachineSpec{
					Resources: v1alpha1.ResourceRequirements{VCPU: 4, MemoryMB: 8192, DiskGB: 50},
					Image: v1alpha1.ImageSource{
						Name:           "ubuntu-22.04",
						ChecksumSHA256: validHash,
					},
				},
			},
			expectError: false,
		},
		{
			name: "Air-gap breach: attempts to fetch external remote URL",
			vm: &v1alpha1.VirtualMachine{
				Metadata: v1alpha1.ObjectMeta{Name: "unauthorized-web-vm"},
				Spec: v1alpha1.VirtualMachineSpec{
					Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 20},
					Image: v1alpha1.ImageSource{
						Name:      "ubuntu-22.04",
						RemoteURL: "https://cloud-images.ubuntu.com/releases/22.04.iso",
					},
				},
			},
			expectError: true,
			errorType:   "airgap",
		},
		{
			name: "Tamper detection: SHA256 mismatch",
			vm: &v1alpha1.VirtualMachine{
				Metadata: v1alpha1.ObjectMeta{Name: "tampered-vm"},
				Spec: v1alpha1.VirtualMachineSpec{
					Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 20},
					Image: v1alpha1.ImageSource{
						Name:           "ubuntu-22.04",
						ChecksumSHA256: "deadbeef00000000000000000000000000000000000000000000000000000000",
					},
				},
			},
			expectError: true,
			errorType:   "airgap",
		},
		{
			name: "Missing image: image not in offline catalog",
			vm: &v1alpha1.VirtualMachine{
				Metadata: v1alpha1.ObjectMeta{Name: "missing-image-vm"},
				Spec: v1alpha1.VirtualMachineSpec{
					Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 2048, DiskGB: 20},
					Image: v1alpha1.ImageSource{
						Name: "non-existent-rhel-9",
					},
				},
			},
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "Resource validation: negative vCPU",
			vm: &v1alpha1.VirtualMachine{
				Metadata: v1alpha1.ObjectMeta{Name: "invalid-cpu-vm"},
				Spec: v1alpha1.VirtualMachineSpec{
					Resources: v1alpha1.ResourceRequirements{VCPU: 0, MemoryMB: 2048, DiskGB: 20},
					Image:     v1alpha1.ImageSource{Name: "ubuntu-22.04"},
				},
			},
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "Naming violation: invalid DNS subdomain name",
			vm: &v1alpha1.VirtualMachine{
				Metadata: v1alpha1.ObjectMeta{Name: "INVALID_UPPERCASE_NAME!"},
				Spec: v1alpha1.VirtualMachineSpec{
					Resources: v1alpha1.ResourceRequirements{VCPU: 1, MemoryMB: 1024, DiskGB: 10},
					Image:     v1alpha1.ImageSource{Name: "ubuntu-22.04"},
				},
			},
			expectError: true,
			errorType:   "validation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(tt.vm)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if tt.errorType == "airgap" {
					if _, ok := err.(*AirGapViolationError); !ok {
						t.Errorf("expected *AirGapViolationError, got %T: %v", err, err)
					}
				} else if tt.errorType == "validation" {
					if _, ok := err.(*ValidationError); !ok {
						t.Errorf("expected *ValidationError, got %T: %v", err, err)
					}
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected validation error: %v", err)
				}
			}
		})
	}
}

