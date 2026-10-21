package validator

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/registry"
)

// RFC 1123 subdomain regex standard for Kubernetes object names.
var validNameRegex = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ResourceLimits defines upper and lower thresholds for VM hardware allocation.
type ResourceLimits struct {
	MinVCPU     int
	MaxVCPU     int
	MinMemoryMB int
	MaxMemoryMB int
	MinDiskGB   int
	MaxDiskGB   int
}

// DefaultLimits provides standard host node constraints.
var DefaultLimits = ResourceLimits{
	MinVCPU:     1,
	MaxVCPU:     64,
	MinMemoryMB: 128,
	MaxMemoryMB: 262144, // 256 GB
	MinDiskGB:   1,
	MaxDiskGB:   4096,   // 4 TB
}

// AirGapViolationError indicates a security breach attempt referencing remote assets.
type AirGapViolationError struct {
	Field   string
	Attempt string
	Reason  string
}

func (e *AirGapViolationError) Error() string {
	return fmt.Sprintf("air-gap security policy violation on field '%s' with value '%s': %s", e.Field, e.Attempt, e.Reason)
}

// ValidationError aggregates non-security constraint failures.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed on field '%s': %s", e.Field, e.Reason)
}

// Validator executes pre-admission checks on VirtualMachine specs.
type Validator struct {
	registry *registry.LocalRegistry
	limits   ResourceLimits
}

// NewValidator creates a validator bound to the local air-gap image registry.
func NewValidator(reg *registry.LocalRegistry, limits *ResourceLimits) *Validator {
	lim := DefaultLimits
	if limits != nil {
		lim = *limits
	}
	return &Validator{
		registry: reg,
		limits:   lim,
	}
}

// Validate executes all pre-flight admission checks.
func (v *Validator) Validate(vm *v1alpha1.VirtualMachine) error {
	if vm == nil {
		return fmt.Errorf("virtual machine manifest cannot be nil")
	}

	// 1. Metadata Validation
	if err := v.validateMetadata(&vm.Metadata); err != nil {
		return err
	}

	// 2. Resource Spec Validation
	if err := v.validateResources(&vm.Spec.Resources); err != nil {
		return err
	}

	// 3. Air-Gap Security & Image Validation
	if err := v.validateAirGapSecurity(&vm.Spec.Image); err != nil {
		return err
	}

	return nil
}

func (v *Validator) validateMetadata(meta *v1alpha1.ObjectMeta) error {
	name := strings.TrimSpace(meta.Name)
	if name == "" {
		return &ValidationError{Field: "metadata.name", Reason: "name is required"}
	}
	if len(name) > 63 {
		return &ValidationError{Field: "metadata.name", Reason: "name must be at most 63 characters"}
	}
	if !validNameRegex.MatchString(name) {
		return &ValidationError{
			Field:  "metadata.name",
			Reason: "name must follow RFC 1123 DNS naming format (lowercase alphanumeric and hyphens only, starting and ending with alphanumeric)",
		}
	}
	return nil
}

func (v *Validator) validateResources(res *v1alpha1.ResourceRequirements) error {
	if res.VCPU < v.limits.MinVCPU || res.VCPU > v.limits.MaxVCPU {
		return &ValidationError{
			Field:  "spec.resources.vcpu",
			Reason: fmt.Sprintf("vCPU must be between %d and %d (received %d)", v.limits.MinVCPU, v.limits.MaxVCPU, res.VCPU),
		}
	}

	if res.MemoryMB < v.limits.MinMemoryMB || res.MemoryMB > v.limits.MaxMemoryMB {
		return &ValidationError{
			Field:  "spec.resources.memoryMb",
			Reason: fmt.Sprintf("memoryMB must be between %d and %d (received %d)", v.limits.MinMemoryMB, v.limits.MaxMemoryMB, res.MemoryMB),
		}
	}

	if res.DiskGB < v.limits.MinDiskGB || res.DiskGB > v.limits.MaxDiskGB {
		return &ValidationError{
			Field:  "spec.resources.diskGb",
			Reason: fmt.Sprintf("diskGB must be between %d and %d (received %d)", v.limits.MinDiskGB, v.limits.MaxDiskGB, res.DiskGB),
		}
	}

	return nil
}

func (v *Validator) validateAirGapSecurity(img *v1alpha1.ImageSource) error {
	// Rule 1: Disallow any remote outbound URLs (Air-Gapped mandate)
	if strings.TrimSpace(img.RemoteURL) != "" {
		return &AirGapViolationError{
			Field:   "spec.image.remoteUrl",
			Attempt: img.RemoteURL,
			Reason:  "outbound network fetching is strictly prohibited in air-gapped environments",
		}
	}

	// Rule 2: Verify local registry image name
	imageName := strings.TrimSpace(img.Name)
	if imageName == "" {
		return &ValidationError{
			Field:  "spec.image.name",
			Reason: "image name is required from local air-gapped registry",
		}
	}

	entry, found := v.registry.GetImage(imageName)
	if !found {
		return &ValidationError{
			Field:  "spec.image.name",
			Reason: fmt.Sprintf("image '%s' is not registered in the offline catalog; images must be pre-loaded into the air-gapped store", imageName),
		}
	}

	// Rule 3: Cryptographic Integrity Verification
	if img.ChecksumSHA256 != "" {
		if !strings.EqualFold(img.ChecksumSHA256, entry.SHA256) {
			return &AirGapViolationError{
				Field:   "spec.image.checksumSha256",
				Attempt: img.ChecksumSHA256,
				Reason: fmt.Sprintf(
					"cryptographic integrity mismatch for image '%s': expected manifest hash '%s' but found '%s' in air-gapped registry",
					imageName, img.ChecksumSHA256, entry.SHA256,
				),
			}
		}
	}

	return nil
}

