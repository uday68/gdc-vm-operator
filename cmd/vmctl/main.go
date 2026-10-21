package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/ipam"
	"github.com/uday68/gdc-vm-operator/pkg/reconciler"
	"github.com/uday68/gdc-vm-operator/pkg/registry"
	"github.com/uday68/gdc-vm-operator/pkg/runtime"
	"github.com/uday68/gdc-vm-operator/pkg/scheduler"
	"github.com/uday68/gdc-vm-operator/pkg/snapshot"
	"github.com/uday68/gdc-vm-operator/pkg/store"
	"github.com/uday68/gdc-vm-operator/pkg/telemetry"
	"github.com/uday68/gdc-vm-operator/pkg/validator"
	"gopkg.in/yaml.v3"
)

const (
	defaultDataDir = "data"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "init":
		cmdInit()
	case "run", "operator":
		cmdRunOperator(args)
	case "apply":
		cmdApply(args)
	case "get":
		cmdGet(args)
	case "describe":
		cmdDescribe(args)
	case "logs":
		cmdLogs(args)
	case "delete":
		cmdDelete(args)
	case "images":
		cmdImages()
	case "capacity", "node":
		cmdCapacity()
	case "snapshot":
		cmdSnapshot(args)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Print(`GDC Air-Gapped Virtual Machine Operator CLI (vmctl)

Usage:
  vmctl <command> [arguments]

Available Commands:
  init                     Bootstrap offline air-gap directories, catalog, and sample images
  run                      Start the continuous Operator Reconciliation Engine daemon
  apply -f <file.yaml>     Submit or update a VirtualMachine manifest
  get vms                  List all managed Virtual Machines
  get vm <name>            Display state of a specific Virtual Machine (-o yaml supported)
  describe <name>          Show detailed specifications, conditions, and health telemetry
  logs <name>              Inspect guest console and boot logs for a VM
  delete <name>            Gracefully terminate and delete a Virtual Machine
  images                   List registered images in the offline air-gapped catalog
  capacity                 Inspect host node compute (CPU, RAM, Disk) and IPAM subnet capacity
  snapshot <action>        Manage offline VM backups (create, list, restore)
`)
}

func getComponents() (store.Store, *registry.LocalRegistry, *validator.Validator, runtime.VMRuntime, *ipam.IPAM, *scheduler.Scheduler) {
	stateDir := filepath.Join(defaultDataDir, "state")
	regDir := filepath.Join(defaultDataDir, "registry")
	logsDir := filepath.Join(defaultDataDir, "logs")

	s, err := store.NewFileStore(stateDir)
	if err != nil {
		fmt.Printf("Failed to open state store: %v\n", err)
		os.Exit(1)
	}

	reg, err := registry.NewLocalRegistry(regDir)
	if err != nil {
		fmt.Printf("Failed to open local registry: %v\n", err)
		os.Exit(1)
	}

	val := validator.NewValidator(reg, nil)

	runnerPath := filepath.Join("bin", "vm-runner.exe")
	if _, err := os.Stat(runnerPath); os.IsNotExist(err) {
		runnerPath = filepath.Join("bin", "vm-runner")
	}

	rt, err := runtime.NewProcessVMRuntime(logsDir, runnerPath)
	if err != nil {
		fmt.Printf("Failed to initialize runtime: %v\n", err)
		os.Exit(1)
	}

	ipamMgr, err := ipam.NewIPAM("10.244.0.0/24")
	if err != nil {
		fmt.Printf("Failed to initialize IPAM: %v\n", err)
		os.Exit(1)
	}
	sched := scheduler.NewScheduler(16, 32768, 1000)

	// Pre-populate reservations & IPAM from active VMs in state
	for _, vm := range s.List() {
		if vm.Status.Phase == v1alpha1.PhaseRunning || vm.Status.Phase == v1alpha1.PhaseProvisioning {
			if err := sched.Reserve(vm); err != nil {
				fmt.Printf("Warning: failed to restore scheduler reservation for VM %s: %v\n", vm.Metadata.Name, err)
			}
			if vm.Status.AllocatedIP != "" {
				if err := ipamMgr.ReserveIP(vm.Metadata.Name, vm.Status.AllocatedIP); err != nil {
					fmt.Printf("Warning: failed to restore IPAM allocation for VM %s: %v\n", vm.Metadata.Name, err)
				}
			}
		}
	}

	return s, reg, val, rt, ipamMgr, sched
}

func cmdInit() {
	fmt.Println("[INIT] Initializing air-gapped environment directories...")
	dirs := []string{
		filepath.Join(defaultDataDir, "state"),
		filepath.Join(defaultDataDir, "logs"),
		filepath.Join(defaultDataDir, "registry", "images"),
		"examples",
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("Error creating directory '%s': %v\n", dir, err)
			return
		}
	}

	// Seed sample offline images
	regDir := filepath.Join(defaultDataDir, "registry")
	reg, err := registry.NewLocalRegistry(regDir)
	if err != nil {
		fmt.Printf("Failed to initialize registry: %v\n", err)
		return
	}

	imagesDir := filepath.Join(defaultDataDir, "registry", "images")
	ubuntuFile := filepath.Join(imagesDir, "ubuntu-22.04.qcow2")
	if _, err := os.Stat(ubuntuFile); os.IsNotExist(err) {
		if err := os.WriteFile(ubuntuFile, []byte("OFFLINE_PREINSTALLED_UBUNTU_22.04_BINARY_IMAGE_PAYLOAD"), 0644); err != nil {
			fmt.Printf("Failed writing sample Ubuntu image payload: %v\n", err)
			return
		}
	}
	uEntry, err := reg.RegisterImage("ubuntu-22.04", "ubuntu-22.04.qcow2", "linux", "Ubuntu Server 22.04 LTS (Air-gapped gold image)")
	if err != nil {
		fmt.Printf("Failed to register sample Ubuntu image: %v\n", err)
		return
	}

	cirrosFile := filepath.Join(imagesDir, "cirros-0.5.2.img")
	if _, err := os.Stat(cirrosFile); os.IsNotExist(err) {
		if err := os.WriteFile(cirrosFile, []byte("OFFLINE_PREINSTALLED_CIRROS_TINY_TEST_PAYLOAD"), 0644); err != nil {
			fmt.Printf("Failed writing sample CirrOS image payload: %v\n", err)
			return
		}
	}
	if _, err := reg.RegisterImage("cirros-0.5.2", "cirros-0.5.2.img", "linux", "CirrOS 0.5.2 Minimal Cloud Test Image"); err != nil {
		fmt.Printf("Failed to register sample CirrOS image: %v\n", err)
		return
	}

	// Create sample example manifests
	validYAML := fmt.Sprintf(`apiVersion: gdc.infrastructure.io/v1alpha1
kind: VirtualMachine
metadata:
  name: database-primary
spec:
  resources:
    vcpu: 2
    memoryMb: 4096
    diskGb: 50
  image:
    name: ubuntu-22.04
    checksumSha256: "%s"
  restartPolicy: Always
  network:
    isolated: true
`, uEntry.SHA256)

	if err := os.WriteFile(filepath.Join("examples", "valid-ubuntu-vm.yaml"), []byte(validYAML), 0644); err != nil {
		fmt.Printf("Failed writing valid-ubuntu-vm.yaml: %v\n", err)
		return
	}

	badAirGapYAML := `apiVersion: gdc.infrastructure.io/v1alpha1
kind: VirtualMachine
metadata:
  name: malicious-remote-fetch
spec:
  resources:
    vcpu: 1
    memoryMb: 1024
    diskGb: 10
  image:
    name: ubuntu-22.04
    remoteUrl: "https://unauthorized-public-mirror.com/image.iso"
  restartPolicy: OnFailure
`
	if err := os.WriteFile(filepath.Join("examples", "airgap-violation.yaml"), []byte(badAirGapYAML), 0644); err != nil {
		fmt.Printf("Failed writing airgap-violation.yaml: %v\n", err)
		return
	}

	fmt.Println("[INIT] Air-gapped registry seeded with 2 offline images:")
	fmt.Printf("       - ubuntu-22.04 (SHA256: %s)\n", uEntry.SHA256)
	fmt.Println("       - cirros-0.5.2")
	fmt.Println("[INIT] Sample manifests written to examples/valid-ubuntu-vm.yaml and examples/airgap-violation.yaml")
	fmt.Println("[SUCCESS] Environment ready! Run 'vmctl run' in one terminal, and 'vmctl apply -f examples/valid-ubuntu-vm.yaml' in another.")
}

func cmdRunOperator(args []string) {
	s, reg, val, rt, ipamMgr, sched := getComponents()
	ctrl := reconciler.NewController(s, val, reg, rt, ipamMgr, sched, 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(args) > 0 && (args[0] == "--once" || args[0] == "-1") {
		fmt.Println("[OPERATOR] Executing single-pass reconciliation...")
		ctrl.ReconcileAll(ctx)
		fmt.Println("[OPERATOR] Single-pass reconciliation complete.")
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("===================================================================")
	fmt.Println(" GDC Air-Gapped Virtual Machine Operator Reconciler Started        ")
	fmt.Println(" Control Loop: Level-triggered ticker every 2s                     ")
	fmt.Println(" Observability: Telemetry endpoints active at http://localhost:8080")
	fmt.Println("   - Liveness Probe:  /healthz                                     ")
	fmt.Println("   - Readiness Probe: /readyz                                      ")
	fmt.Println("   - Metrics:         /metrics (Prometheus format)                 ")
	fmt.Println(" Press Ctrl+C to terminate cleanly                                 ")
	fmt.Println("===================================================================")

	// Launch Prometheus and Health Telemetry server
	telemetrySrv := telemetry.NewServer(8080, s)
	go func() {
		_ = telemetrySrv.Start(ctx)
	}()

	go ctrl.Start(ctx)

	<-sigChan
	fmt.Println("\n[OPERATOR] Shutting down controller reconciler...")
	ctrl.Stop()
	cancel()
	fmt.Println("[OPERATOR] Reconciler terminated cleanly.")
}

func cmdApply(args []string) {
	if len(args) < 2 || args[0] != "-f" {
		fmt.Println("Usage: vmctl apply -f <path-to-manifest.yaml>")
		return
	}

	filePath := args[1]
	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Failed to read file '%s': %v\n", filePath, err)
		return
	}

	var vm v1alpha1.VirtualMachine
	if err := yaml.Unmarshal(data, &vm); err != nil {
		fmt.Printf("Failed to parse YAML manifest: %v\n", err)
		return
	}

	if vm.Metadata.Name == "" {
		fmt.Println("Error: manifest metadata.name cannot be empty")
		return
	}

	s, _, _, _, _, _ := getComponents()

	// If new, set initial status
	existing, _ := s.Get(vm.Metadata.Name)
	if existing == nil {
		vm.Metadata.CreatedAt = time.Now()
		vm.Status.Phase = v1alpha1.PhasePending
		vm.Status.LastTransitionTime = time.Now()
		vm.Status.Message = "VirtualMachine submitted to operator admission queue"
	} else {
		// Preserve status during spec update
		vm.Status = existing.Status
	}

	if err := s.Save(&vm); err != nil {
		fmt.Printf("Failed to apply VirtualMachine: %v\n", err)
		return
	}

	fmt.Printf("virtualmachine.gdc.infrastructure.io/%s configured\n", vm.Metadata.Name)
}

func cmdGet(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: vmctl get [vms | vm <name>]")
		return
	}

	target := args[0]
	s, _, _, _, _, _ := getComponents()

	if target == "vms" || target == "vm" && len(args) == 1 {
		vms := s.List()
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tPHASE\tVCPU\tMEMORY\tPID\tIP\tRESTARTS\tAGE")
		for _, vm := range vms {
			age := time.Since(vm.Metadata.CreatedAt).Truncate(time.Second)
			fmt.Fprintf(w, "%s\t%s\t%d\t%dMB\t%d\t%s\t%d\t%s\n",
				vm.Metadata.Name,
				vm.Status.Phase,
				vm.Spec.Resources.VCPU,
				vm.Spec.Resources.MemoryMB,
				vm.Status.PID,
				vm.Status.AllocatedIP,
				vm.Status.RestartCount,
				age,
			)
		}
		w.Flush()
		return
	}

	if target == "vm" && len(args) >= 2 {
		name := args[1]
		vm, err := s.Get(name)
		if err != nil {
			fmt.Println(err)
			return
		}

		if len(args) >= 4 && args[2] == "-o" && args[3] == "yaml" {
			data, _ := yaml.Marshal(vm)
			fmt.Println(string(data))
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tPHASE\tVCPU\tMEMORY\tPID\tIP\tRESTARTS")
		fmt.Fprintf(w, "%s\t%s\t%d\t%dMB\t%d\t%s\t%d\n",
			vm.Metadata.Name,
			vm.Status.Phase,
			vm.Spec.Resources.VCPU,
			vm.Spec.Resources.MemoryMB,
			vm.Status.PID,
			vm.Status.AllocatedIP,
			vm.Status.RestartCount,
		)
		w.Flush()
	}
}

func cmdDescribe(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: vmctl describe <name>")
		return
	}

	name := args[0]
	s, _, _, _, _, _ := getComponents()

	vm, err := s.Get(name)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Name:         %s\n", vm.Metadata.Name)
	fmt.Printf("API Version:  %s\n", vm.APIVersion)
	fmt.Printf("Kind:         %s\n", vm.Kind)
	fmt.Printf("Created:      %s\n", vm.Metadata.CreatedAt.Format(time.RFC3339))
	fmt.Println("Spec:")
	fmt.Printf("  Resources:  vCPU: %d | Memory: %dMB | Disk: %dGB\n", vm.Spec.Resources.VCPU, vm.Spec.Resources.MemoryMB, vm.Spec.Resources.DiskGB)
	fmt.Printf("  Image:      Name: %s\n", vm.Spec.Image.Name)
	if vm.Spec.Image.ChecksumSHA256 != "" {
		fmt.Printf("              SHA256 Checksum: %s\n", vm.Spec.Image.ChecksumSHA256)
	}
	if vm.Spec.Image.RemoteURL != "" {
		fmt.Printf("              Remote URL (Air-Gap Disallowed): %s\n", vm.Spec.Image.RemoteURL)
	}
	fmt.Printf("  Restart:    %s\n", vm.Spec.RestartPolicy)
	fmt.Println("Status:")
	fmt.Printf("  Phase:      %s\n", vm.Status.Phase)
	fmt.Printf("  Host PID:   %d\n", vm.Status.PID)
	fmt.Printf("  IP Address: %s\n", vm.Status.AllocatedIP)
	fmt.Printf("  Restarts:   %d\n", vm.Status.RestartCount)
	fmt.Printf("  Message:    %s\n", vm.Status.Message)
	fmt.Println("Conditions:")
	for _, c := range vm.Status.Conditions {
		fmt.Printf("  - Type: %-12s Status: %-6s Reason: %-16s Message: %s\n", c.Type, c.Status, c.Reason, c.Message)
	}
}

func cmdLogs(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: vmctl logs <name>")
		return
	}
	name := args[0]
	_, _, _, rt, _, _ := getComponents()
	logs, err := rt.GetLogs(name)
	if err != nil {
		fmt.Printf("Failed to get logs: %v\n", err)
		return
	}
	fmt.Println(strings.TrimSpace(logs))
}

func cmdDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: vmctl delete <name>")
		return
	}
	name := args[0]
	s, _, _, _, _, _ := getComponents()
	vm, err := s.Get(name)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Trigger graceful termination phase
	vm.Status.Phase = v1alpha1.PhaseTerminating
	vm.Status.Message = "Termination requested by user"
	if err := s.Save(vm); err != nil {
		fmt.Printf("Failed marking VM for termination: %v\n", err)
		return
	}
	fmt.Printf("virtualmachine.gdc.infrastructure.io/%s marked for termination\n", name)
}

func cmdImages() {
	_, reg, _, _, _, _ := getComponents()
	images := reg.ListImages()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tFILE\tOS\tSIZE (BYTES)\tSHA256 CHECKSUM")
	for _, img := range images {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", img.Name, img.Filename, img.OSFamily, img.SizeBytes, img.SHA256)
	}
	w.Flush()
}

func cmdCapacity() {
	_, _, _, _, ipamMgr, sched := getComponents()
	cap := sched.CapacityReport()
	allocIPs, totalIPs := ipamMgr.Stats()

	fmt.Println("GDC Host Node Resource Capacity & Network Report:")
	fmt.Println("-------------------------------------------------")
	fmt.Printf("vCPU:       Total: %-4d | Allocated: %-4d | Available: %-4d (%.1f%% utilized)\n",
		cap.TotalVCPU, cap.AllocatedVCPU, cap.AvailableVCPU, float64(cap.AllocatedVCPU)/float64(cap.TotalVCPU)*100)
	fmt.Printf("Memory:     Total: %-4dMB| Allocated: %-4dMB| Available: %-4dMB(%.1f%% utilized)\n",
		cap.TotalMemoryMB, cap.AllocatedMemory, cap.AvailableMemory, float64(cap.AllocatedMemory)/float64(cap.TotalMemoryMB)*100)
	fmt.Printf("Disk:       Total: %-4dGB| Allocated: %-4dGB| Available: %-4dGB(%.1f%% utilized)\n",
		cap.TotalDiskGB, cap.AllocatedDisk, cap.AvailableDisk, float64(cap.AllocatedDisk)/float64(cap.TotalDiskGB)*100)
	fmt.Printf("IPAM Pool:  Subnet: 10.244.0.0/24 | Allocated IPs: %d | Available IPs: %d\n",
		allocIPs, totalIPs-allocIPs)
}

func cmdSnapshot(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: vmctl snapshot [create <vm> <name> | list <vm> | restore <vm> <name> <dest-path>]")
		return
	}

	snapDir := filepath.Join(defaultDataDir, "snapshots")
	mgr, err := snapshot.NewManager(snapDir)
	if err != nil {
		fmt.Printf("Failed to open snapshot manager: %v\n", err)
		return
	}

	action := args[0]
	vmName := args[1]

	s, reg, _, _, _, _ := getComponents()
	vm, err := s.Get(vmName)
	if err != nil && action != "restore" {
		fmt.Println(err)
		return
	}

	switch action {
	case "create":
		if len(args) < 3 {
			fmt.Println("Usage: vmctl snapshot create <vm-name> <snapshot-name>")
			return
		}
		snapName := args[2]
		entry, found := reg.GetImage(vm.Spec.Image.Name)
		if !found {
			fmt.Printf("Disk image '%s' not found in registry\n", vm.Spec.Image.Name)
			return
		}
		sourceDisk, err := reg.GetImagePath(entry.Filename)
		if err != nil {
			fmt.Printf("Failed getting disk path: %v\n", err)
			return
		}
		meta, err := mgr.Create(vm, snapName, sourceDisk)
		if err != nil {
			fmt.Printf("Failed to create snapshot: %v\n", err)
			return
		}
		fmt.Printf("Snapshot '%s' created successfully for VM '%s'\n", snapName, vmName)
		fmt.Printf("SHA256 Checksum: %s (Size: %d bytes)\n", meta.SHA256Checksum, meta.SizeBytes)

	case "list":
		list, err := mgr.List(vmName)
		if err != nil {
			fmt.Printf("Failed to list snapshots: %v\n", err)
			return
		}
		if len(list) == 0 {
			fmt.Printf("No snapshots found for VM '%s'\n", vmName)
			return
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "SNAPSHOT NAME\tVM NAME\tCREATED\tSIZE (BYTES)\tSHA256 CHECKSUM")
		for _, s := range list {
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
				s.Name, s.VMName, s.Timestamp.Format(time.RFC3339), s.SizeBytes, s.SHA256Checksum)
		}
		w.Flush()

	case "restore":
		if len(args) < 4 {
			fmt.Println("Usage: vmctl snapshot restore <vm-name> <snapshot-name> <target-disk-path> [--force]")
			return
		}
		snapName := args[2]
		targetPath := args[3]
		allowOverwrite := len(args) >= 5 && (args[4] == "--force" || args[4] == "--overwrite")
		meta, err := mgr.Restore(vmName, snapName, targetPath, allowOverwrite)
		if err != nil {
			fmt.Printf("Failed to restore snapshot: %v\n", err)
			return
		}
		fmt.Printf("Restored snapshot '%s' to '%s' (Verified SHA256: %s)\n",
			snapName, targetPath, meta.SHA256Checksum)

	default:
		fmt.Printf("Unknown snapshot action '%s'\n", action)
	}
}
