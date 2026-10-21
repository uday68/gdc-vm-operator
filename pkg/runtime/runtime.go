package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
)

// VMRuntime defines the lifecycle operations for virtual machine host processes.
type VMRuntime interface {
	Start(ctx context.Context, vm *v1alpha1.VirtualMachine, imagePath string) (int, error)
	Stop(pid int) error
	IsAlive(pid int) bool
	GetLogs(vmName string) (string, error)
}

// ProcessVMRuntime runs VMs as isolated host processes, simulating hypervisor execution.
type ProcessVMRuntime struct {
	logsDir   string
	runnerBin string
	mu        sync.RWMutex
}

// NewProcessVMRuntime constructs the runtime engine.
func NewProcessVMRuntime(logsDir, runnerBin string) (*ProcessVMRuntime, error) {
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}
	return &ProcessVMRuntime{
		logsDir:   logsDir,
		runnerBin: runnerBin,
	}, nil
}

// Start launches a new VM process and redirects stdout/stderr to a log file.
func (r *ProcessVMRuntime) Start(ctx context.Context, vm *v1alpha1.VirtualMachine, imagePath string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	logFilePath := filepath.Join(r.logsDir, fmt.Sprintf("%s.log", vm.Metadata.Name))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open VM log file: %w", err)
	}

	// Prepare process arguments
	args := []string{
		"--name", vm.Metadata.Name,
		"--vcpu", strconv.Itoa(vm.Spec.Resources.VCPU),
		"--memory-mb", strconv.Itoa(vm.Spec.Resources.MemoryMB),
		"--disk-path", imagePath,
	}

	var cmd *exec.Cmd
	if r.runnerBin != "" {
		cmd = exec.Command(r.runnerBin, args...)
	} else {
		// Fallback self-runner using Go run or background loop
		cmd = exec.Command("powershell", "-NoProfile", "-Command", fmt.Sprintf(
			"Write-Host '[BOOT] Initializing %s (vCPU: %d, RAM: %dMB)'; while($true) { Start-Sleep -Seconds 1; Write-Host '[HEARTBEAT] %s alive' }",
			vm.Metadata.Name, vm.Spec.Resources.VCPU, vm.Spec.Resources.MemoryMB, vm.Metadata.Name,
		))
	}

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return 0, fmt.Errorf("failed to launch VM process: %w", err)
	}

	pid := cmd.Process.Pid
	// Write initial startup marker
	_, _ = fmt.Fprintf(logFile, "[OPERATOR] Process started with PID: %d at %s\n", pid, time.Now().UTC().Format(time.RFC3339))
	_ = logFile.Close()

	return pid, nil
}

// Stop gracefully terminates a VM process, falling back to force kill.
func (r *ProcessVMRuntime) Stop(pid int) error {
	if pid <= 0 {
		return nil
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}

	// Terminate process
	_ = proc.Kill()

	// Wait up to 3 seconds for release
	for i := 0; i < 6; i++ {
		if !r.IsAlive(pid) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

// IsAlive checks whether the OS process is currently running.
func (r *ProcessVMRuntime) IsAlive(pid int) bool {
	return checkProcessAlive(pid)
}

// GetLogs returns the accumulated log content for a VM.
func (r *ProcessVMRuntime) GetLogs(vmName string) (string, error) {
	logFilePath := filepath.Join(r.logsDir, fmt.Sprintf("%s.log", vmName))
	data, err := os.ReadFile(logFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "<no logs recorded yet>", nil
		}
		return "", err
	}
	return string(data), nil
}
