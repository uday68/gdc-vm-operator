package scheduler

import (
	"fmt"
	"sync"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
)

// NodeCapacity describes total allocatable compute resources on the physical node.
type NodeCapacity struct {
	TotalVCPU        int `json:"totalVcpu"`
	TotalMemoryMB    int `json:"totalMemoryMb"`
	TotalDiskGB      int `json:"totalDiskGb"`
	AllocatedVCPU    int `json:"allocatedVcpu"`
	AllocatedMemory  int `json:"allocatedMemory"`
	AllocatedDisk    int `json:"allocatedDisk"`
	AvailableVCPU    int `json:"availableVcpu"`
	AvailableMemory  int `json:"availableMemory"`
	AvailableDisk    int `json:"availableDisk"`
}

// Scheduler governs resource admission to prevent host node overcommit.
type Scheduler struct {
	totalVCPU    int
	totalMemMB   int
	totalDiskGB  int
	reservations map[string]v1alpha1.ResourceRequirements // vmName -> resources
	mu           sync.RWMutex
}

// NewScheduler initializes the host capacity scheduler.
func NewScheduler(totalVCPU, totalMemMB, totalDiskGB int) *Scheduler {
	if totalVCPU <= 0 {
		totalVCPU = 16
	}
	if totalMemMB <= 0 {
		totalMemMB = 32768 // 32 GB
	}
	if totalDiskGB <= 0 {
		totalDiskGB = 1000 // 1 TB
	}

	return &Scheduler{
		totalVCPU:    totalVCPU,
		totalMemMB:   totalMemMB,
		totalDiskGB:  totalDiskGB,
		reservations: make(map[string]v1alpha1.ResourceRequirements),
	}
}

// CanSchedule checks if host node has sufficient capacity to run the requested VM.
// If the VM already has a reservation, it checks whether updating to the new spec is possible.
func (s *Scheduler) CanSchedule(vm *v1alpha1.VirtualMachine) (bool, string) {
	if vm == nil {
		return false, "vm cannot be nil"
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	req := vm.Spec.Resources
	currentVCPU, currentMem, currentDisk := s.currentAllocationsLocked()

	// If VM already holds reservation, compute against the delta
	if existing, exists := s.reservations[vm.Metadata.Name]; exists {
		if existing.VCPU == req.VCPU && existing.MemoryMB == req.MemoryMB && existing.DiskGB == req.DiskGB {
			return true, ""
		}
		currentVCPU -= existing.VCPU
		currentMem -= existing.MemoryMB
		currentDisk -= existing.DiskGB
	}

	if currentVCPU+req.VCPU > s.totalVCPU {
		return false, fmt.Sprintf("insufficient vCPU: requested %d, available %d (node total %d)",
			req.VCPU, s.totalVCPU-currentVCPU, s.totalVCPU)
	}

	if currentMem+req.MemoryMB > s.totalMemMB {
		return false, fmt.Sprintf("insufficient memory: requested %dMB, available %dMB (node total %dMB)",
			req.MemoryMB, s.totalMemMB-currentMem, s.totalMemMB)
	}

	if currentDisk+req.DiskGB > s.totalDiskGB {
		return false, fmt.Sprintf("insufficient disk: requested %dGB, available %dGB (node total %dGB)",
			req.DiskGB, s.totalDiskGB-currentDisk, s.totalDiskGB)
	}

	return true, ""
}

// Reserve allocates node capacity for the VM or safely updates an existing reservation.
// It is idempotent if the resource requirements have not changed.
// If requirements changed, it verifies that the node can accommodate the delta.
// If the delta exceeds capacity, it fails with an error and retains the previous reservation.
func (s *Scheduler) Reserve(vm *v1alpha1.VirtualMachine) error {
	if vm == nil {
		return fmt.Errorf("vm cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	req := vm.Spec.Resources
	existing, exists := s.reservations[vm.Metadata.Name]
	if exists {
		if existing.VCPU == req.VCPU && existing.MemoryMB == req.MemoryMB && existing.DiskGB == req.DiskGB {
			// Idempotent: exact same requirements already reserved
			return nil
		}

		// Calculate allocations excluding the current VM's reservation
		currentVCPU, currentMem, currentDisk := s.currentAllocationsLocked()
		baseVCPU := currentVCPU - existing.VCPU
		baseMem := currentMem - existing.MemoryMB
		baseDisk := currentDisk - existing.DiskGB

		if baseVCPU+req.VCPU > s.totalVCPU ||
			baseMem+req.MemoryMB > s.totalMemMB ||
			baseDisk+req.DiskGB > s.totalDiskGB {
			return fmt.Errorf("insufficient node capacity to update reservation for %s (requested: vCPU %d, RAM %dMB, Disk %dGB; available: vCPU %d, RAM %dMB, Disk %dGB)",
				vm.Metadata.Name, req.VCPU, req.MemoryMB, req.DiskGB,
				s.totalVCPU-baseVCPU, s.totalMemMB-baseMem, s.totalDiskGB-baseDisk)
		}

		s.reservations[vm.Metadata.Name] = req
		return nil
	}

	currentVCPU, currentMem, currentDisk := s.currentAllocationsLocked()
	if currentVCPU+req.VCPU > s.totalVCPU ||
		currentMem+req.MemoryMB > s.totalMemMB ||
		currentDisk+req.DiskGB > s.totalDiskGB {
		return fmt.Errorf("insufficient node capacity to reserve resources for %s (requested: vCPU %d, RAM %dMB, Disk %dGB; available: vCPU %d, RAM %dMB, Disk %dGB)",
			vm.Metadata.Name, req.VCPU, req.MemoryMB, req.DiskGB,
			s.totalVCPU-currentVCPU, s.totalMemMB-currentMem, s.totalDiskGB-currentDisk)
	}

	s.reservations[vm.Metadata.Name] = req
	return nil
}

// GetReservation returns the current reservation for a VM, if one exists.
func (s *Scheduler) GetReservation(vmName string) (v1alpha1.ResourceRequirements, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res, exists := s.reservations[vmName]
	return res, exists
}

// HasReservation checks if a VM holds an active reservation.
func (s *Scheduler) HasReservation(vmName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.reservations[vmName]
	return exists
}

// Release frees node capacity when a VM terminates.
func (s *Scheduler) Release(vmName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reservations, vmName)
}

// CapacityReport generates telemetry metrics on node utilization.
func (s *Scheduler) CapacityReport() NodeCapacity {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, m, d := s.currentAllocationsLocked()
	return NodeCapacity{
		TotalVCPU:       s.totalVCPU,
		TotalMemoryMB:   s.totalMemMB,
		TotalDiskGB:     s.totalDiskGB,
		AllocatedVCPU:   v,
		AllocatedMemory: m,
		AllocatedDisk:   d,
		AvailableVCPU:   s.totalVCPU - v,
		AvailableMemory: s.totalMemMB - m,
		AvailableDisk:   s.totalDiskGB - d,
	}
}

func (s *Scheduler) currentAllocationsLocked() (vcpu, mem, disk int) {
	for _, res := range s.reservations {
		vcpu += res.VCPU
		mem += res.MemoryMB
		disk += res.DiskGB
	}
	return vcpu, mem, disk
}

