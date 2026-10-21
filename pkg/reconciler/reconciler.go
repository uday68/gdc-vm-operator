package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/ipam"
	"github.com/uday68/gdc-vm-operator/pkg/registry"
	"github.com/uday68/gdc-vm-operator/pkg/runtime"
	"github.com/uday68/gdc-vm-operator/pkg/scheduler"
	"github.com/uday68/gdc-vm-operator/pkg/store"
	"github.com/uday68/gdc-vm-operator/pkg/validator"
)

const MaxCrashRestarts = 3

// Controller manages the continuous state reconciliation of Virtual Machines.
type Controller struct {
	store     store.Store
	validator *validator.Validator
	registry  *registry.LocalRegistry
	runtime   runtime.VMRuntime
	ipam      *ipam.IPAM
	scheduler *scheduler.Scheduler
	interval  time.Duration
	stopChan  chan struct{}
	wg        sync.WaitGroup
	mu        sync.Mutex
}

// NewController builds a new Reconciler controller instance.
func NewController(
	s store.Store,
	v *validator.Validator,
	reg *registry.LocalRegistry,
	rt runtime.VMRuntime,
	ip *ipam.IPAM,
	sched *scheduler.Scheduler,
	interval time.Duration,
) *Controller {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if ip == nil {
		ip, _ = ipam.NewIPAM("10.244.0.0/24")
	}
	if sched == nil {
		sched = scheduler.NewScheduler(16, 32768, 1000)
	}
	return &Controller{
		store:     s,
		validator: v,
		registry:  reg,
		runtime:   rt,
		ipam:      ip,
		scheduler: sched,
		interval:  interval,
		stopChan:  make(chan struct{}),
	}
}

// Start initiates the reconciliation control loop.
func (c *Controller) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			c.ReconcileAll(ctx)
		}
	}
}

// Stop gracefully shuts down the background control loop.
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stopChan:
		// already stopped
	default:
		close(c.stopChan)
	}
}

// ReconcileAll runs one reconciliation pass across all registered VM resources.
func (c *Controller) ReconcileAll(ctx context.Context) {
	vms := c.store.List()
	for _, vm := range vms {
		if err := c.ReconcileOne(ctx, vm); err != nil {
			// Structured log without halting peer reconciliation
			slog.Error("Failed reconciling virtual machine", "name", vm.Metadata.Name, "error", err)
		}
	}
}

// ReconcileOne executes the core state machine for a single VirtualMachine resource.
func (c *Controller) ReconcileOne(ctx context.Context, vm *v1alpha1.VirtualMachine) error {
	switch vm.Status.Phase {
	case "", v1alpha1.PhasePending, v1alpha1.PhaseValidating:
		return c.handleValidation(vm)

	case v1alpha1.PhaseProvisioning:
		return c.handleProvisioning(ctx, vm)

	case v1alpha1.PhaseRunning:
		return c.handleRunningHealth(ctx, vm)

	case v1alpha1.PhaseTerminating:
		return c.handleTerminating(ctx, vm)

	case v1alpha1.PhaseFailed:
		// Ensure failure state holds no leaked capacity or IP reservations
		c.unwindResources(vm.Metadata.Name)
		return nil

	case v1alpha1.PhaseDegraded, v1alpha1.PhaseStopped:
		// Terminal or halted states: no active reconciler transition unless user intervenes
		return nil

	default:
		return nil
	}
}

func (c *Controller) unwindResources(name string) {
	c.scheduler.Release(name)
	c.ipam.Release(name)
}

func (c *Controller) handleValidation(vm *v1alpha1.VirtualMachine) error {
	if err := c.validator.Validate(vm); err != nil {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = fmt.Sprintf("Admission validation failed: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Validated", v1alpha1.ConditionFalse, "ValidationError", err.Error())
		return c.store.Save(vm)
	}

	vm.SetCondition("Validated", v1alpha1.ConditionTrue, "AdmissionSuccess", "All air-gap and hardware constraints satisfied")

	// Host Node Capacity Admission (Scheduler)
	canSchedule, reason := c.scheduler.CanSchedule(vm)
	if !canSchedule {
		vm.Status.Phase = v1alpha1.PhasePending
		vm.Status.Message = fmt.Sprintf("Queued: %s", reason)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Scheduled", v1alpha1.ConditionFalse, "InsufficientCapacity", reason)
		return c.store.Save(vm)
	}

	// Propagate reservation failure
	if err := c.scheduler.Reserve(vm); err != nil {
		vm.Status.Phase = v1alpha1.PhasePending
		vm.Status.Message = fmt.Sprintf("Reservation failed: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Scheduled", v1alpha1.ConditionFalse, "ReservationFailed", err.Error())
		return c.store.Save(vm)
	}

	vm.SetCondition("Scheduled", v1alpha1.ConditionTrue, "CapacityReserved", "Host resources allocated")

	vm.Status.Phase = v1alpha1.PhaseProvisioning
	vm.Status.Message = "Validation passed & host resources reserved. Scheduling image attachment."
	vm.Status.LastTransitionTime = time.Now().UTC()
	return c.store.Save(vm)
}

func (c *Controller) handleProvisioning(ctx context.Context, vm *v1alpha1.VirtualMachine) error {
	entry, found := c.registry.GetImage(vm.Spec.Image.Name)
	if !found {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = fmt.Sprintf("Image %s not found in registry catalog", vm.Spec.Image.Name)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Provisioned", v1alpha1.ConditionFalse, "ImageNotFound", vm.Status.Message)
		return c.store.Save(vm)
	}

	// Deterministic IP Allocation via Subnet IPAM
	ip, err := c.ipam.Allocate(vm.Metadata.Name)
	if err != nil {
		vm.Status.Phase = v1alpha1.PhaseProvisioning
		vm.Status.Message = fmt.Sprintf("Network allocation paused: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("NetworkReady", v1alpha1.ConditionFalse, "IPAMPoolExhausted", err.Error())
		return c.store.Save(vm)
	}

	imagePath, err := c.registry.GetImagePath(entry.Filename)
	if err != nil {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = fmt.Sprintf("Unsafe image path: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Provisioned", v1alpha1.ConditionFalse, "UnsafeImagePath", err.Error())
		return c.store.Save(vm)
	}

	pid, err := c.runtime.Start(ctx, vm, imagePath)
	if err != nil {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.AllocatedIP = ""
		vm.Status.Message = fmt.Sprintf("Runtime process start failed: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Ready", v1alpha1.ConditionFalse, "RuntimeStartError", err.Error())
		return c.store.Save(vm)
	}

	vm.Status.Phase = v1alpha1.PhaseRunning
	vm.Status.PID = pid
	vm.Status.AllocatedIP = ip
	vm.Status.Message = fmt.Sprintf("Virtual machine active on host with PID %d, IP %s", pid, ip)
	vm.Status.LastTransitionTime = time.Now().UTC()
	vm.SetCondition("Ready", v1alpha1.ConditionTrue, "VMRunning", vm.Status.Message)
	vm.SetCondition("NetworkReady", v1alpha1.ConditionTrue, "IPAllocated", fmt.Sprintf("Bound to subnet IP %s", ip))
	return c.store.Save(vm)
}

func (c *Controller) handleRunningHealth(ctx context.Context, vm *v1alpha1.VirtualMachine) error {
	// Inspect if process is still active
	if c.runtime.IsAlive(vm.Status.PID) {
		// VM is healthy
		return nil
	}

	// Process died! Self-healing reconciliation triggered.
	slog.Warn("Process crash detected, triggering self-healing recovery",
		"vm", vm.Metadata.Name,
		"crashedPID", vm.Status.PID,
		"previousRestarts", vm.Status.RestartCount,
	)

	policy := vm.Spec.RestartPolicy
	if policy == "" {
		policy = v1alpha1.RestartPolicyAlways
	}

	if policy == v1alpha1.RestartPolicyNever {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseStopped
		vm.Status.Message = "Process exited, RestartPolicy is Never"
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Ready", v1alpha1.ConditionFalse, "ProcessExited", vm.Status.Message)
		return c.store.Save(vm)
	}

	// Check restart threshold
	if vm.Status.RestartCount >= MaxCrashRestarts {
		vm.Status.Phase = v1alpha1.PhaseDegraded
		vm.Status.Message = fmt.Sprintf("Max restart attempts (%d) exceeded. Marked as Degraded (CrashLoopBackoff)", MaxCrashRestarts)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Ready", v1alpha1.ConditionFalse, "CrashLoopBackoff", vm.Status.Message)
		return c.store.Save(vm)
	}

	// Attempt self-healing restart
	entry, found := c.registry.GetImage(vm.Spec.Image.Name)
	if !found {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = "Image vanished during restart attempt"
		vm.Status.LastTransitionTime = time.Now().UTC()
		return c.store.Save(vm)
	}

	imagePath, err := c.registry.GetImagePath(entry.Filename)
	if err != nil {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = fmt.Sprintf("Invalid image path: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		return c.store.Save(vm)
	}

	newPid, err := c.runtime.Start(ctx, vm, imagePath)
	if err != nil {
		vm.Status.Message = fmt.Sprintf("Self-healing restart attempt failed: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.Status.RestartCount++
		if vm.Status.RestartCount >= MaxCrashRestarts {
			vm.Status.Phase = v1alpha1.PhaseDegraded
		}
		return c.store.Save(vm)
	}

	vm.Status.PID = newPid
	vm.Status.RestartCount++
	vm.Status.Message = fmt.Sprintf("Self-healed: restarted with new PID %d (Restart count: %d)", newPid, vm.Status.RestartCount)
	vm.Status.LastTransitionTime = time.Now().UTC()
	vm.SetCondition("Ready", v1alpha1.ConditionTrue, "SelfHealedRestart", vm.Status.Message)
	return c.store.Save(vm)
}

func (c *Controller) handleTerminating(ctx context.Context, vm *v1alpha1.VirtualMachine) error {
	if vm.Status.PID > 0 {
		if err := c.runtime.Stop(vm.Status.PID); err != nil {
			slog.Warn("Failed stopping process during termination", "vm", vm.Metadata.Name, "pid", vm.Status.PID, "error", err)
		}
	}
	// Release network IP and host capacity owned strictly by this VM
	c.unwindResources(vm.Metadata.Name)
	// Object cleaned up from store
	return c.store.Delete(vm.Metadata.Name)
}
