package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/pathutil"
)

// VMRuntime defines the lifecycle operations for virtual machine host processes.
type VMRuntime interface {
	Start(ctx context.Context, vm *v1alpha1.VirtualMachine, imagePath string) (int, error)
	Stop(pid int) error
	IsAlive(pid int) bool
	GetExitCode(vmName string) (int, bool)
	GetLogs(vmName string) (string, error)
}

// ProcessVMRuntime executes isolated guest simulation processes to demonstrate VM workload lifecycles.
// Note: This is a prototype workload simulation and does not control bare-metal hypervisors or production KVM VMs.
type ProcessVMRuntime struct {
	logsDir   string
	runnerBin string
	processes map[string]*exec.Cmd
	exitCodes map[string]int
	mu        sync.RWMutex
}

// NewProcessVMRuntime constructs the simulation runtime engine.
func NewProcessVMRuntime(logsDir, runnerBin string) (*ProcessVMRuntime, error) {
	if strings.TrimSpace(logsDir) == "" {
		return nil, fmt.Errorf("logs directory cannot be empty")
	}

	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	return &ProcessVMRuntime{
		logsDir:   logsDir,
		runnerBin: runnerBin,
		processes: make(map[string]*exec.Cmd),
		exitCodes: make(map[string]int),
	}, nil
}

// Start launches a new simulated VM process and redirects stdout/stderr to a log file.
// Broad locks are avoided: filesystem checks and process startup occur outside critical sections.
func (r *ProcessVMRuntime) Start(ctx context.Context, vm *v1alpha1.VirtualMachine, imagePath string) (int, error) {
	if vm == nil {
		return 0, fmt.Errorf("virtual machine manifest cannot be nil")
	}
	if err := pathutil.ValidateResourceName(vm.Metadata.Name); err != nil {
		return 0, fmt.Errorf("invalid VM name for runtime: %w", err)
	}

	// Verify disk image artifact exists on host filesystem before launching workload (lock-free)
	if _, err := os.Stat(imagePath); err != nil {
		return 0, fmt.Errorf("disk image '%s' is inaccessible or does not exist: %w", imagePath, err)
	}

	logFilePath, err := pathutil.SafeJoin(r.logsDir, fmt.Sprintf("%s.log", vm.Metadata.Name))
	if err != nil {
		return 0, fmt.Errorf("unsafe log file path: %w", err)
	}

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open VM log file: %w", err)
	}

	// Clean up previous exit status file if exists
	exitFilePath, _ := pathutil.SafeJoin(r.logsDir, fmt.Sprintf("%s.exit", vm.Metadata.Name))
	if exitFilePath != "" {
		_ = os.Remove(exitFilePath)
	}

	// Prepare simulation process arguments
	args := []string{
		"--name", vm.Metadata.Name,
		"--vcpu", strconv.Itoa(vm.Spec.Resources.VCPU),
		"--memory-mb", strconv.Itoa(vm.Spec.Resources.MemoryMB),
		"--disk-path", imagePath,
	}

	// Optional simulation control overrides via metadata labels for failure injection
	if vm.Metadata.Labels != nil {
		if exitAfter, ok := vm.Metadata.Labels["simulation.exit-after"]; ok && exitAfter != "" {
			args = append(args, "--exit-after", exitAfter)
		}
		if exitCode, ok := vm.Metadata.Labels["simulation.exit-code"]; ok && exitCode != "" {
			args = append(args, "--exit-code", exitCode)
		}
	}

	var cmd *exec.Cmd
	if r.runnerBin != "" {
		cmd = exec.CommandContext(ctx, r.runnerBin, args...)
	} else {
		// Fallback self-runner loop
		cmd = exec.CommandContext(ctx, "powershell", "-NoProfile", "-Command", fmt.Sprintf(
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
	// Record initial startup marker and flush
	startMsg := fmt.Sprintf("[OPERATOR] Process started with PID: %d at %s\n", pid, time.Now().UTC().Format(time.RFC3339))
	if _, err := logFile.WriteString(startMsg); err != nil {
		_ = logFile.Close()
		return pid, fmt.Errorf("failed writing startup marker to log: %w", err)
	}

	if err := logFile.Sync(); err != nil {
		_ = logFile.Close()
		return pid, fmt.Errorf("failed syncing log file: %w", err)
	}

	if err := logFile.Close(); err != nil {
		return pid, fmt.Errorf("failed closing log file: %w", err)
	}

	// Record running process in internal map under short lock
	r.mu.Lock()
	r.processes[vm.Metadata.Name] = cmd
	delete(r.exitCodes, vm.Metadata.Name)
	r.mu.Unlock()

	// Launch background monitor to capture exit status without blocking
	go func(runningCmd *exec.Cmd, vmName, exitPath string) {
		waitErr := runningCmd.Wait()
		code := 0
		if waitErr != nil {
			if exitErr, ok := waitErr.(*exec.ExitError); ok {
				code = exitErr.ExitCode()
			} else {
				code = 1
			}
		}

		r.mu.Lock()
		r.exitCodes[vmName] = code
		delete(r.processes, vmName)
		r.mu.Unlock()

		if exitPath != "" {
			_ = os.WriteFile(exitPath, []byte(strconv.Itoa(code)), 0644)
		}
	}(cmd, vm.Metadata.Name, exitFilePath)

	return pid, nil
}

// GetExitCode retrieves the process exit code for a terminated VM, reading from disk if needed across restarts.
func (r *ProcessVMRuntime) GetExitCode(vmName string) (int, bool) {
	r.mu.RLock()
	code, ok := r.exitCodes[vmName]
	r.mu.RUnlock()
	if ok {
		return code, true
	}

	// Check durable .exit file on disk (written upon process termination)
	exitFilePath, err := pathutil.SafeJoin(r.logsDir, fmt.Sprintf("%s.exit", vmName))
	if err == nil {
		data, err := os.ReadFile(exitFilePath)
		if err == nil {
			parsed, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err == nil {
				r.mu.Lock()
				r.exitCodes[vmName] = parsed
				r.mu.Unlock()
				return parsed, true
			}
		}
	}

	return 0, false
}

// Stop gracefully terminates a simulated VM process, falling back to force kill.
func (r *ProcessVMRuntime) Stop(pid int) error {
	if pid <= 0 {
		return nil
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}
	defer func() {
		_ = proc.Release()
	}()

	// Terminate process
	if err := proc.Kill(); err != nil && r.IsAlive(pid) {
		return fmt.Errorf("failed killing process PID %d: %w", pid, err)
	}

	// Wait up to 3 seconds for release
	for i := 0; i < 6; i++ {
		if !r.IsAlive(pid) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	if r.IsAlive(pid) {
		return fmt.Errorf("timed out waiting for process PID %d to terminate", pid)
	}

	return nil
}

// IsAlive checks whether the OS process is currently running.
func (r *ProcessVMRuntime) IsAlive(pid int) bool {
	return checkProcessAlive(pid)
}

// GetLogs returns the accumulated log content for a VM.
func (r *ProcessVMRuntime) GetLogs(vmName string) (string, error) {
	if err := pathutil.ValidateResourceName(vmName); err != nil {
		return "", fmt.Errorf("invalid VM name: %w", err)
	}

	logFilePath, err := pathutil.SafeJoin(r.logsDir, fmt.Sprintf("%s.log", vmName))
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(logFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "<no logs recorded yet>", nil
		}
		return "", err
	}
	return string(data), nil
}
