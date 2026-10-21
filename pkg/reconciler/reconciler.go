package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/uday68/gdc-vm-operator/api/v1alpha1"
	"github.com/uday68/gdc-vm-operator/pkg/ipam"
	"github.com/uday68/gdc-vm-operator/pkg/registry"
	"github.com/uday68/gdc-vm-operator/pkg/runtime"
	"github.com/uday68/gdc-vm-operator/pkg/scheduler"
	"github.com/uday68/gdc-vm-operator/pkg/store"
	"github.com/uday68/gdc-vm-operator/pkg/validator"
	"github.com/uday68/gdc-vm-operator/pkg/workqueue"
)

const MaxCrashRestarts = 3

// QueueKey generates the stable unique key used to identify a VM in the workqueue.
func QueueKey(vm *v1alpha1.VirtualMachine) string {
	if vm == nil {
		return ""
	}
	if vm.Metadata.Namespace != "" {
		return fmt.Sprintf("%s/%s", vm.Metadata.Namespace, vm.Metadata.Name)
	}
	return vm.Metadata.Name
}

// SplitQueueKey parses a workqueue key into its namespace and name components.
func SplitQueueKey(key string) (namespace, name string) {
	parts := strings.Split(key, "/")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", key
}

// Controller manages the continuous state reconciliation of Virtual Machines.
type Controller struct {
	store     store.Store
	validator *validator.Validator
	registry  *registry.LocalRegistry
	runtime   runtime.VMRuntime
	ipam      *ipam.IPAM
	scheduler *scheduler.Scheduler
	queue     workqueue.WorkQueue
	workers   int
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
		queue:     workqueue.NewQueue(nil),
		workers:   2,
		interval:  interval,
		stopChan:  make(chan struct{}),
	}
}

// SetWorkers configures the number of concurrent queue workers.
func (c *Controller) SetWorkers(workers int) *Controller {
	c.mu.Lock()
	defer c.mu.Unlock()
	if workers <= 0 {
		workers = 1
	}
	c.workers = workers
	return c
}

// Queue returns the controller's underlying workqueue.
func (c *Controller) Queue() workqueue.WorkQueue {
	return c.queue
}

// Enqueue adds a VM to the workqueue for reconciliation.
func (c *Controller) Enqueue(vm *v1alpha1.VirtualMachine) {
	if vm == nil {
		return
	}
	key := QueueKey(vm)
	if key != "" {
		c.queue.Add(key)
	}
}

// EnqueueKey adds a VM key directly to the workqueue.
func (c *Controller) EnqueueKey(key string) {
	if key != "" {
		c.queue.Add(key)
	}
}

// Start initiates the reconciliation control loop with worker processing.
func (c *Controller) Start(ctx context.Context) {
	// 1. Reconcile and reconstruct state on startup before entering loop
	if err := c.ReconcileRecoveredState(ctx); err != nil {
		slog.Error("Failed reconciling recovered state on startup", "error", err)
	}

	// 2. Enqueue all recovered VMs initially into workqueue
	for _, vm := range c.store.List() {
		c.Enqueue(vm)
	}

	// 3. Start worker pool
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go func(workerID int) {
			defer c.wg.Done()
			c.runWorker(ctx, workerID)
		}(i)
	}

	// 4. Background level-triggered ticker enqueuing items into workqueue
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.Stop()
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			for _, vm := range c.store.List() {
				c.Enqueue(vm)
			}
		}
	}
}

func (c *Controller) runWorker(ctx context.Context, workerID int) {
	for {
		key, shutdown := c.queue.Get()
		if shutdown {
			return
		}

		err := c.processKey(ctx, key)
		c.queue.Done(key)

		if err != nil {
			slog.Error("Worker failed reconciling VM", "workerID", workerID, "key", key, "error", err)
			if c.queue.NumRequeues(key) < MaxCrashRestarts*2 {
				delay := c.queue.AddRateLimited(key)
				slog.Debug("Requeued VM with rate limiting", "key", key, "delay", delay)
			} else {
				c.queue.Forget(key)
				slog.Error("Exceeded max retry attempts, dropping VM key from queue", "key", key)
			}
		} else {
			c.queue.Forget(key)
		}
	}
}

func (c *Controller) processKey(ctx context.Context, key string) error {
	_, name := SplitQueueKey(key)
	vm, err := c.store.Get(name)
	if err != nil {
		// VM was deleted from store; no state to reconcile
		return nil
	}
	return c.ReconcileOne(ctx, vm)
}

// Stop gracefully shuts down the worker pool, stops the workqueue, and waits for worker termination.
func (c *Controller) Stop() {
	c.mu.Lock()
	select {
	case <-c.stopChan:
		c.mu.Unlock()
		return
	default:
		close(c.stopChan)
	}
	c.mu.Unlock()

	// Shut down workqueue: stops intake, cancels delayed retries, and broadcasts to unblock all workers
	c.queue.ShutDown()

	// Wait for all worker goroutines to complete
	c.wg.Wait()
}

// RuntimeObservation captures live facts discovered dynamically during reconciliation.
type RuntimeObservation struct {
	ProcessAlive     bool
	ExitCode         int
	ExitCodeKnown    bool
	ImageExists      bool
	ImagePath        string
	CapacityReserved bool
	IPAllocated      bool
	AllocatedIP      string
}

// Observe performs read-only live runtime discovery without taking mutative action.
func (c *Controller) Observe(ctx context.Context, vm *v1alpha1.VirtualMachine) *RuntimeObservation {
	obs := &RuntimeObservation{}
	if vm.Status.PID > 0 {
		obs.ProcessAlive = c.runtime.IsAlive(vm.Status.PID)
		if !obs.ProcessAlive {
			obs.ExitCode, obs.ExitCodeKnown = c.runtime.GetExitCode(vm.Metadata.Name)
		}
	}
	if entry, found := c.registry.GetImage(vm.Spec.Image.Name); found {
		obs.ImageExists = true
		obs.ImagePath, _ = c.registry.GetImagePath(entry.Filename)
	}
	obs.CapacityReserved = c.scheduler.HasReservation(vm.Metadata.Name)
	obs.AllocatedIP, obs.IPAllocated = c.ipam.GetAllocatedIP(vm.Metadata.Name)
	return obs
}

// ReconcileRecoveredState reconstructs scheduler reservations and IP allocations from persisted state.
// Inconsistencies (e.g. overcapacity or IP conflicts) are detected and surfaced as actionable conditions.
func (c *Controller) ReconcileRecoveredState(ctx context.Context) error {
	vms := c.store.List()
	for _, vm := range vms {
		switch vm.Status.Phase {
		case v1alpha1.PhaseRunning, v1alpha1.PhaseProvisioning:
			processAlive := vm.Status.PID > 0 && c.runtime.IsAlive(vm.Status.PID)

			// 1. Reconstruct scheduler reservation
			if err := c.scheduler.Reserve(vm); err != nil {
				slog.Error("Recovery: host capacity overcommit detected for VM", "name", vm.Metadata.Name, "error", err)
				vm.Status.Phase = v1alpha1.PhaseDegraded
				vm.SetCondition("Scheduled", v1alpha1.ConditionFalse, "RecoveryCapacityExceeded", err.Error())
				vm.Status.Message = fmt.Sprintf("Recovery failed: %v", err)
				vm.Status.LastTransitionTime = time.Now().UTC()
				_ = c.store.Save(vm)
				continue
			}

			// 2. Reconstruct IP allocation
			if vm.Status.AllocatedIP != "" {
				if err := c.ipam.ReserveIP(vm.Metadata.Name, vm.Status.AllocatedIP); err != nil {
					slog.Error("Recovery: IP allocation conflict detected for VM", "name", vm.Metadata.Name, "ip", vm.Status.AllocatedIP, "error", err)
					c.scheduler.Release(vm.Metadata.Name)
					vm.Status.Phase = v1alpha1.PhaseDegraded
					vm.SetCondition("NetworkReady", v1alpha1.ConditionFalse, "IPConflictDetected", err.Error())
					vm.Status.Message = fmt.Sprintf("Recovery IP conflict: %v", err)
					vm.Status.LastTransitionTime = time.Now().UTC()
					_ = c.store.Save(vm)
					continue
				}
			}

			// 3. Check process health
			if vm.Status.Phase == v1alpha1.PhaseRunning && !processAlive {
				vm.SetCondition("Ready", v1alpha1.ConditionFalse, "ProcessTerminatedWhileOffline", "Guest process was not running on controller startup")
				_ = c.store.Save(vm)
			}

		case v1alpha1.PhaseFailed, v1alpha1.PhaseStopped:
			// Guarantee terminal phases hold no reservations
			c.unwindResources(vm.Metadata.Name)

		case v1alpha1.PhaseTerminating:
			_ = c.handleTerminating(ctx, vm)
		}
	}
	return nil
}

// ReconcileAll runs one reconciliation pass across all registered VM resources.
func (c *Controller) ReconcileAll(ctx context.Context) {
	vms := c.store.List()
	for _, vm := range vms {
		if err := c.ReconcileOne(ctx, vm); err != nil {
			slog.Error("Failed reconciling virtual machine", "name", vm.Metadata.Name, "error", err)
		}
	}
}

// ReconcileOne executes the core state machine for a single VirtualMachine resource.
func (c *Controller) ReconcileOne(ctx context.Context, vm *v1alpha1.VirtualMachine) error {
	obs := c.Observe(ctx, vm)

	switch vm.Status.Phase {
	case "", v1alpha1.PhasePending, v1alpha1.PhaseValidating:
		return c.handleValidation(vm)

	case v1alpha1.PhaseProvisioning:
		return c.handleProvisioning(ctx, vm, obs)

	case v1alpha1.PhaseRunning:
		return c.handleRunningHealth(ctx, vm, obs)

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

func (c *Controller) handleProvisioning(ctx context.Context, vm *v1alpha1.VirtualMachine, obs *RuntimeObservation) error {
	if !obs.ImageExists {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = fmt.Sprintf("Image %s not found in registry catalog", vm.Spec.Image.Name)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Provisioned", v1alpha1.ConditionFalse, "ImageNotFound", vm.Status.Message)
		return c.store.Save(vm)
	}

	if obs.ImagePath == "" {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = "Unsafe image path"
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Provisioned", v1alpha1.ConditionFalse, "UnsafeImagePath", vm.Status.Message)
		return c.store.Save(vm)
	}

	// Deterministic IP Allocation via Subnet IPAM
	var ip string
	var err error
	if vm.Status.AllocatedIP != "" {
		if err = c.ipam.ReserveIP(vm.Metadata.Name, vm.Status.AllocatedIP); err == nil {
			ip = vm.Status.AllocatedIP
		}
	}
	if ip == "" {
		ip, err = c.ipam.Allocate(vm.Metadata.Name)
	}

	if err != nil {
		vm.Status.Phase = v1alpha1.PhaseProvisioning
		vm.Status.Message = fmt.Sprintf("Network allocation paused: %v", err)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("NetworkReady", v1alpha1.ConditionFalse, "IPAMPoolExhausted", err.Error())
		return c.store.Save(vm)
	}

	pid, err := c.runtime.Start(ctx, vm, obs.ImagePath)
	if err != nil {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.AllocatedIP = ""
		vm.Status.PID = 0
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

func (c *Controller) handleRunningHealth(ctx context.Context, vm *v1alpha1.VirtualMachine, obs *RuntimeObservation) error {
	// 1. Check if spec resources were modified while running
	if currentRes, hasRes := c.scheduler.GetReservation(vm.Metadata.Name); hasRes {
		req := vm.Spec.Resources
		if currentRes.VCPU != req.VCPU || currentRes.MemoryMB != req.MemoryMB || currentRes.DiskGB != req.DiskGB {
			if err := c.scheduler.Reserve(vm); err != nil {
				vm.SetCondition("Scheduled", v1alpha1.ConditionFalse, "SpecUpdateFailed", err.Error())
				_ = c.store.Save(vm)
			} else {
				vm.SetCondition("Scheduled", v1alpha1.ConditionTrue, "SpecUpdateApplied", "Resource reservation updated to match spec")
				_ = c.store.Save(vm)
			}
		}
	}

	// 2. Inspect if process is still active
	if obs.ProcessAlive {
		return nil
	}

	// 3. Process is dead. Analyze exit status and restart policy.
	policy := vm.Spec.RestartPolicy
	if policy == "" {
		policy = v1alpha1.RestartPolicyAlways
	}

	isCleanExit := obs.ExitCodeKnown && obs.ExitCode == 0

	slog.Warn("Process termination observed",
		"vm", vm.Metadata.Name,
		"crashedPID", vm.Status.PID,
		"exitCode", obs.ExitCode,
		"exitCodeKnown", obs.ExitCodeKnown,
		"policy", policy,
	)

	// Policy: Never
	if policy == v1alpha1.RestartPolicyNever {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.PID = 0
		if isCleanExit {
			vm.Status.Phase = v1alpha1.PhaseStopped
			vm.Status.Message = "Workload completed cleanly (RestartPolicy: Never)"
			vm.Status.LastTransitionTime = time.Now().UTC()
			vm.SetCondition("Ready", v1alpha1.ConditionFalse, "Completed", vm.Status.Message)
		} else {
			vm.Status.Phase = v1alpha1.PhaseFailed
			vm.Status.Message = fmt.Sprintf("Workload crashed with exit code %d (RestartPolicy: Never)", obs.ExitCode)
			vm.Status.LastTransitionTime = time.Now().UTC()
			vm.SetCondition("Ready", v1alpha1.ConditionFalse, "CrashNonZeroExit", vm.Status.Message)
		}
		return c.store.Save(vm)
	}

	// Policy: OnFailure with clean exit
	if policy == v1alpha1.RestartPolicyOnFailure && isCleanExit {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.PID = 0
		vm.Status.Phase = v1alpha1.PhaseStopped
		vm.Status.Message = "Workload completed cleanly (RestartPolicy: OnFailure)"
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Ready", v1alpha1.ConditionFalse, "Completed", vm.Status.Message)
		return c.store.Save(vm)
	}

	// For Always, or OnFailure with non-zero exit: self-healing restart
	if vm.Status.RestartCount >= MaxCrashRestarts {
		vm.Status.Phase = v1alpha1.PhaseDegraded
		vm.Status.Message = fmt.Sprintf("Max restart attempts (%d) exceeded. Marked as Degraded (CrashLoopBackoff)", MaxCrashRestarts)
		vm.Status.LastTransitionTime = time.Now().UTC()
		vm.SetCondition("Ready", v1alpha1.ConditionFalse, "CrashLoopBackoff", vm.Status.Message)
		return c.store.Save(vm)
	}

	// Rate-limit restarts to prevent tight spin loops if process crashes immediately
	minRestartInterval := 200 * time.Millisecond
	if elapsed := time.Since(vm.Status.LastTransitionTime); elapsed < minRestartInterval {
		select {
		case <-time.After(minRestartInterval - elapsed):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if !obs.ImageExists || obs.ImagePath == "" {
		c.unwindResources(vm.Metadata.Name)
		vm.Status.Phase = v1alpha1.PhaseFailed
		vm.Status.Message = "Image vanished during restart attempt"
		vm.Status.LastTransitionTime = time.Now().UTC()
		return c.store.Save(vm)
	}

	newPid, err := c.runtime.Start(ctx, vm, obs.ImagePath)
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
