// Package main implements vm-runner, a lightweight guest workload simulation process.
// It is intended for testing and demonstration of VM lifecycle orchestration (boot, heartbeat,
// graceful signal handling, and shutdown) without requiring hypervisor or root privileges.
// It does NOT run actual hypervisor virtual machines or containerized workloads.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	name := flag.String("name", "unknown-vm", "VM name")
	vcpu := flag.Int("vcpu", 1, "Number of vCPUs")
	memoryMB := flag.Int("memory-mb", 512, "Memory in MB")
	diskPath := flag.String("disk-path", "/dev/null", "Path to disk image")
	flag.Parse()

	ipLastOctet := 10 + rand.Intn(200)
	startTime := time.Now()

	fmt.Printf("[BOOT] Starting Virtual Machine: %s\n", *name)
	fmt.Printf("[BOOT] Hardware: %d vCPUs, %d MB RAM\n", *vcpu, *memoryMB)
	fmt.Printf("[BOOT] Storage: Attached offline disk from %s\n", *diskPath)
	fmt.Println("[KERNEL] Initializing Air-Gapped Minimal Hypervisor runtime")
	fmt.Printf("[NETWORK] Internal host-only network bound to 10.244.0.%d\n", ipLastOctet)
	fmt.Println("[SYSTEMD] All guest services started successfully. System ready.")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case sig := <-sigChan:
			fmt.Printf("[SHUTDOWN] Received signal (%s), flushing disk buffers...\n", sig)
			time.Sleep(200 * time.Millisecond)
			fmt.Println("[SHUTDOWN] VM terminated cleanly.")
			os.Exit(0)
		case <-ticker.C:
			uptime := time.Since(startTime).Truncate(time.Second)
			fmt.Printf("[HEARTBEAT] VM '%s' status=HEALTHY, uptime=%s\n", *name, uptime)
		}
	}
}

