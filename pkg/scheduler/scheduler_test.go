package scheduler

import (
	"testing"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
)

func TestScheduler_CapacityAdmissionAndRelease(t *testing.T) {
	// Node with 4 vCPUs, 8192 MB RAM, 100 GB Disk
	sched := NewScheduler(4, 8192, 100)

	vm1 := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "vm-large"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 3, MemoryMB: 6144, DiskGB: 50},
		},
	}

	// 1. CanSchedule VM1
	can, reason := sched.CanSchedule(vm1)
	if !can {
		t.Fatalf("expected VM1 to be schedulable, got rejected: %s", reason)
	}

	// 2. Reserve for VM1
	if err := sched.Reserve(vm1); err != nil {
		t.Fatalf("failed to reserve for VM1: %v", err)
	}

	// 3. VM2 requests 2 vCPUs (Node only has 1 vCPU left)
	vm2 := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "vm-overflow"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 2, MemoryMB: 1024, DiskGB: 10},
		},
	}
	can2, reason2 := sched.CanSchedule(vm2)
	if can2 {
		t.Fatalf("expected VM2 to be rejected due to CPU overcommit, but was admitted")
	}
	if reason2 == "" {
		t.Errorf("expected detailed rejection reason, got empty string")
	}

	// 4. Release VM1 and verify VM2 can now be scheduled
	sched.Release("vm-large")
	can2After, _ := sched.CanSchedule(vm2)
	if !can2After {
		t.Fatalf("expected VM2 to be schedulable after VM1 was released")
	}
}

