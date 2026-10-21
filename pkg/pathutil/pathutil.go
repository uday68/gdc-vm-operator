package pathutil

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// RFC 1123 DNS subdomain format: lowercase alphanumeric, hyphens, and dots.
// Each dot-separated segment must be an RFC 1123 label (lowercase alphanumeric and hyphens,
// starting and ending with alphanumeric, 1-63 chars). Total length up to 253 characters.
var dnsSubdomainRegex = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// Windows reserved device names (case-insensitive).
var windowsReservedNames = map[string]struct{}{
	"con":  {},
	"prn":  {},
	"aux":  {},
	"nul":  {},
	"com1": {},
	"com2": {},
	"com3": {},
	"com4": {},
	"com5": {},
	"com6": {},
	"com7": {},
	"com8": {},
	"com9": {},
	"lpt1": {},
	"lpt2": {},
	"lpt3": {},
	"lpt4": {},
	"lpt5": {},
	"lpt6": {},
	"lpt7": {},
	"lpt8": {},
	"lpt9": {},
}

// ValidateResourceName verifies that a resource identifier (VM, snapshot, image) is safe
// across all platforms and immune to path traversal.
func ValidateResourceName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if len(name) > 253 {
		return fmt.Errorf("name '%s' exceeds maximum length of 253 characters", name)
	}
	if !dnsSubdomainRegex.MatchString(name) {
		return fmt.Errorf("name '%s' must match RFC 1123 DNS format (lowercase alphanumeric, hyphens, and dots, starting and ending with an alphanumeric character)", name)
	}

	for _, label := range strings.Split(name, ".") {
		if len(label) > 63 {
			return fmt.Errorf("label '%s' in name '%s' exceeds maximum length of 63 characters", label, name)
		}
	}

	// Reject Windows device names even on POSIX for cross-platform portability.
	// Check the base prefix before any dot as well as the full name.
	baseLower := strings.ToLower(name)
	parts := strings.Split(baseLower, ".")
	if _, isReserved := windowsReservedNames[parts[0]]; isReserved {
		return fmt.Errorf("name '%s' is a reserved system device name and cannot be used", name)
	}

	return nil
}

// ValidateFilename ensures an artifact filename has no directory traversal components.
func ValidateFilename(filename string) error {
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return fmt.Errorf("filename cannot be empty")
	}
	if strings.ContainsAny(filename, `/\:*?"<>|`) || strings.Contains(filename, "\x00") {
		return fmt.Errorf("filename '%s' contains invalid path or separator characters", filename)
	}
	if filename == "." || filename == ".." {
		return fmt.Errorf("filename cannot be '.' or '..'")
	}
	if filepath.Base(filename) != filename {
		return fmt.Errorf("filename '%s' must not contain path traversal", filename)
	}

	ext := strings.ToLower(filepath.Ext(filename))
	baseWithoutExt := strings.TrimSuffix(strings.ToLower(filename), ext)
	if _, isReserved := windowsReservedNames[baseWithoutExt]; isReserved {
		return fmt.Errorf("filename '%s' uses a reserved system device name", filename)
	}

	return nil
}

// SafeJoin securely joins baseDir with an element and verifies the resulting path
// cannot escape baseDir.
func SafeJoin(baseDir, elem string) (string, error) {
	if strings.TrimSpace(baseDir) == "" {
		return "", fmt.Errorf("base directory cannot be empty")
	}
	if strings.TrimSpace(elem) == "" {
		return "", fmt.Errorf("path element cannot be empty")
	}

	cleanElem := filepath.Clean(elem)
	// Reject absolute paths or paths with volume names
	if filepath.IsAbs(cleanElem) || filepath.VolumeName(cleanElem) != "" || strings.HasPrefix(cleanElem, `\`) || strings.HasPrefix(cleanElem, `/`) {
		return "", fmt.Errorf("path element must be relative: %s", elem)
	}

	cleanBase, err := filepath.Abs(filepath.Clean(baseDir))
	if err != nil {
		return "", fmt.Errorf("invalid base directory: %w", err)
	}

	joined := filepath.Join(cleanBase, cleanElem)
	cleanJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", fmt.Errorf("invalid path assembly: %w", err)
	}

	rel, err := filepath.Rel(cleanBase, cleanJoined)
	if err != nil || strings.HasPrefix(rel, "..") || rel == "." && elem != "." {
		return "", fmt.Errorf("path traversal attempt detected: element '%s' escapes base directory '%s'", elem, baseDir)
	}

	return cleanJoined, nil
}
