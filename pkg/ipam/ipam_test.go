package ipam

import (
	"fmt"
	"testing"
)

func TestIPAM_Lifecycle(t *testing.T) {
	// Subnet /29 gives 8 total IPs:
	// .0 (network), .1 (gateway), .2-.6 (5 usable host IPs), .7 (broadcast)
	allocator, err := NewIPAM("10.244.1.0/29")
	if err != nil {
		t.Fatalf("failed to create IPAM: %v", err)
	}

	// 1. Allocate for VM-1
	ip1, err := allocator.Allocate("vm-1")
	if err != nil {
		t.Fatalf("failed to allocate IP: %v", err)
	}
	if ip1 != "10.244.1.2" {
		t.Errorf("expected 10.244.1.2, got %s", ip1)
	}

	// 2. Test Idempotency (asking for vm-1 again returns same IP)
	ip1Retry, _ := allocator.Allocate("vm-1")
	if ip1Retry != ip1 {
		t.Errorf("expected idempotent IP %s, got %s", ip1, ip1Retry)
	}

	// 3. Allocate remaining 4 usable host IPs
	for i := 2; i <= 5; i++ {
		vmName := fmt.Sprintf("vm-%d", i)
		ip, err := allocator.Allocate(vmName)
		if err != nil {
			t.Fatalf("unexpected allocation failure for %s: %v", vmName, err)
		}
		expectedIP := fmt.Sprintf("10.244.1.%d", i+1)
		if ip != expectedIP {
			t.Errorf("expected %s, got %s", expectedIP, ip)
		}
	}

	// 4. Pool Exhaustion Check: 6th VM must fail
	_, err = allocator.Allocate("vm-overflow")
	if err == nil {
		t.Fatalf("expected pool exhaustion error, got nil")
	}

	// 5. Release an IP and verify immediate reusability
	allocator.Release("vm-3") // should release 10.244.1.4
	reclaimedIP, err := allocator.Allocate("vm-new")
	if err != nil {
		t.Fatalf("failed to allocate reclaimed IP: %v", err)
	}
	if reclaimedIP != "10.244.1.4" {
		t.Errorf("expected reclaimed IP 10.244.1.4, got %s", reclaimedIP)
	}
}

func TestIPAM_ReserveIPAndConflictDetection(t *testing.T) {
	allocator, err := NewIPAM("10.244.1.0/29")
	if err != nil {
		t.Fatalf("failed to create IPAM: %v", err)
	}

	// 1. Valid specific IP reservation
	if err := allocator.ReserveIP("vm-alpha", "10.244.1.5"); err != nil {
		t.Fatalf("failed to reserve valid IP: %v", err)
	}
	ip, exists := allocator.GetAllocatedIP("vm-alpha")
	if !exists || ip != "10.244.1.5" {
		t.Fatalf("expected 10.244.1.5 to be assigned, got: %s (exists=%v)", ip, exists)
	}

	// 2. Idempotent reservation for same VM
	if err := allocator.ReserveIP("vm-alpha", "10.244.1.5"); err != nil {
		t.Fatalf("expected idempotent reservation to succeed, got: %v", err)
	}

	// 3. Conflict detection: vm-beta attempts to claim vm-alpha's IP
	if err := allocator.ReserveIP("vm-beta", "10.244.1.5"); err == nil {
		t.Fatalf("expected conflict error when vm-beta claims allocated IP, got nil")
	}

	// 4. Reject network, gateway, and broadcast IPs
	if err := allocator.ReserveIP("vm-bad", "10.244.1.0"); err == nil {
		t.Fatalf("expected error allocating network IP .0, got nil")
	}
	if err := allocator.ReserveIP("vm-bad", "10.244.1.1"); err == nil {
		t.Fatalf("expected error allocating gateway IP .1, got nil")
	}
	if err := allocator.ReserveIP("vm-bad", "10.244.1.7"); err == nil {
		t.Fatalf("expected error allocating broadcast IP .7, got nil")
	}

	// 5. Reject IP outside subnet
	if err := allocator.ReserveIP("vm-bad", "192.168.1.100"); err == nil {
		t.Fatalf("expected error allocating out-of-subnet IP, got nil")
	}
}


