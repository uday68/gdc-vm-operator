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

func TestScheduler_IdempotencyAndSpecUpdates(t *testing.T) {
	// Node capacity: 8 vCPUs, 16384 MB RAM, 500 GB Disk
	sched := NewScheduler(8, 16384, 500)

	vm := &v1alpha1.VirtualMachine{
		Metadata: v1alpha1.ObjectMeta{Name: "worker-node"},
		Spec: v1alpha1.VirtualMachineSpec{
			Resources: v1alpha1.ResourceRequirements{VCPU: 4, MemoryMB: 8192, DiskGB: 100},
		},
	}

	// 1. Initial reservation
	if err := sched.Reserve(vm); err != nil {
		t.Fatalf("initial reservation failed: %v", err)
	}
	cap := sched.CapacityReport()
	if cap.AllocatedVCPU != 4 || cap.AvailableVCPU != 4 {
		t.Fatalf("expected 4 vCPU allocated, got: %d", cap.AllocatedVCPU)
	}

	// 2. Repeated reservation must be idempotent (no double allocation)
	if err := sched.Reserve(vm); err != nil {
		t.Fatalf("repeated reservation failed: %v", err)
	}
	cap = sched.CapacityReport()
	if cap.AllocatedVCPU != 4 {
		t.Fatalf("double allocation detected! expected 4 vCPU allocated, got: %d", cap.AllocatedVCPU)
	}

	// 3. Valid spec update: increase from 4 to 6 vCPUs (within 8 vCPU limit)
	vm.Spec.Resources.VCPU = 6
	if err := sched.Reserve(vm); err != nil {
		t.Fatalf("expected valid spec update to succeed, got: %v", err)
	}
	cap = sched.CapacityReport()
	if cap.AllocatedVCPU != 6 || cap.AvailableVCPU != 2 {
		t.Fatalf("expected 6 vCPU allocated after update, got: %d", cap.AllocatedVCPU)
	}

	// 4. Invalid spec update: increase from 6 to 12 vCPUs (exceeds node 8 vCPU total)
	vm.Spec.Resources.VCPU = 12
	err := sched.Reserve(vm)
	if err == nil {
		t.Fatalf("expected overcommit update to fail, but succeeded")
	}

	// 5. Verify the previous valid reservation (6 vCPUs) was NOT lost
	cap = sched.CapacityReport()
	if cap.AllocatedVCPU != 6 {
		t.Fatalf("previous reservation lost on failed update! expected 6 vCPU, got: %d", cap.AllocatedVCPU)
	}
	res, found := sched.GetReservation("worker-node")
	if !found || res.VCPU != 6 {
		t.Fatalf("expected preserved reservation of 6 vCPUs, got: %+v", res)
	}
}


