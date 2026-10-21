package ipam

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

// IPAM manages deterministic IP address allocation within an air-gapped CIDR block.
type IPAM struct {
	cidr       string
	network    *net.IPNet
	startIP    net.IP
	totalIPs   uint32
	allocated  map[string]string // IP -> VM Name
	vmToIP     map[string]string // VM Name -> IP
	mu         sync.RWMutex
}

// NewIPAM creates an IP address allocator for the specified CIDR subnet.
func NewIPAM(cidr string) (*IPAM, error) {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("invalid CIDR subnet '%s': %w", cidr, err)
	}

	ones, bits := ipNet.Mask.Size()
	total := uint32(1 << (bits - ones))
	if total <= 3 {
		return nil, fmt.Errorf("CIDR subnet '%s' is too small (requires > 3 hosts)", cidr)
	}

	return &IPAM{
		cidr:      cidr,
		network:   ipNet,
		startIP:   ipNet.IP,
		totalIPs:  total,
		allocated: make(map[string]string),
		vmToIP:    make(map[string]string),
	}, nil
}

// Allocate assigns the next available internal IP to the specified VM.
func (ipam *IPAM) Allocate(vmName string) (string, error) {
	ipam.mu.Lock()
	defer ipam.mu.Unlock()

	// If VM already has an assigned IP, return idempotently
	if ip, exists := ipam.vmToIP[vmName]; exists {
		return ip, nil
	}

	baseInt := binary.BigEndian.Uint32(ipam.startIP.To4())

	// Reserve network address (0), gateway (1), and broadcast (total - 1)
	for i := uint32(2); i < ipam.totalIPs-1; i++ {
		candidateInt := baseInt + i
		candidateIP := make(net.IP, 4)
		binary.BigEndian.PutUint32(candidateIP, candidateInt)
		candidateStr := candidateIP.String()

		if _, taken := ipam.allocated[candidateStr]; !taken {
			ipam.allocated[candidateStr] = vmName
			ipam.vmToIP[vmName] = candidateStr
			return candidateStr, nil
		}
	}

	return "", fmt.Errorf("IP address pool exhausted in subnet %s (all %d host IPs allocated)", ipam.cidr, ipam.totalIPs-3)
}

// ReserveIP explicitly reserves or recovers a specific IP address for a VM.
// Used during crash recovery and state reconstruction to ensure that a VM
// retains its previously allocated IP without collision.
func (ipam *IPAM) ReserveIP(vmName, ipStr string) error {
	if vmName == "" {
		return fmt.Errorf("vmName cannot be empty")
	}
	parsedIP := net.ParseIP(ipStr)
	if parsedIP == nil || parsedIP.To4() == nil {
		return fmt.Errorf("invalid IPv4 address: '%s'", ipStr)
	}

	ipam.mu.Lock()
	defer ipam.mu.Unlock()

	if !ipam.network.Contains(parsedIP) {
		return fmt.Errorf("IP %s is outside subnet CIDR %s", ipStr, ipam.cidr)
	}

	baseInt := binary.BigEndian.Uint32(ipam.startIP.To4())
	ipInt := binary.BigEndian.Uint32(parsedIP.To4())

	// Network address (0), gateway (1), and broadcast (totalIPs - 1) are reserved
	if ipInt == baseInt {
		return fmt.Errorf("cannot allocate network address %s", ipStr)
	}
	if ipInt == baseInt+1 {
		return fmt.Errorf("cannot allocate default gateway address %s", ipStr)
	}
	if ipInt >= baseInt+ipam.totalIPs-1 {
		return fmt.Errorf("cannot allocate broadcast address %s", ipStr)
	}

	// Check if already allocated to another VM
	if existingVM, taken := ipam.allocated[ipStr]; taken {
		if existingVM == vmName {
			// Idempotent recovery
			return nil
		}
		return fmt.Errorf("IP %s already allocated to VM '%s' (conflict with '%s')", ipStr, existingVM, vmName)
	}

	// If this VM already held a different IP, release the old one
	if oldIP, hasOld := ipam.vmToIP[vmName]; hasOld && oldIP != ipStr {
		delete(ipam.allocated, oldIP)
	}

	ipam.allocated[ipStr] = vmName
	ipam.vmToIP[vmName] = ipStr
	return nil
}

// Release reclaims an IP address allocated to a VM back into the pool.
func (ipam *IPAM) Release(vmName string) {
	ipam.mu.Lock()
	defer ipam.mu.Unlock()

	if ip, exists := ipam.vmToIP[vmName]; exists {
		delete(ipam.allocated, ip)
		delete(ipam.vmToIP, vmName)
	}
}

// GetAllocatedIP returns the IP currently assigned to a VM, if any.
func (ipam *IPAM) GetAllocatedIP(vmName string) (string, bool) {
	ipam.mu.RLock()
	defer ipam.mu.RUnlock()
	ip, exists := ipam.vmToIP[vmName]
	return ip, exists
}

// Stats returns allocation capacity metrics for telemetry.
func (ipam *IPAM) Stats() (allocated int, total int) {
	ipam.mu.RLock()
	defer ipam.mu.RUnlock()
	return len(ipam.allocated), int(ipam.totalIPs - 3)
}

