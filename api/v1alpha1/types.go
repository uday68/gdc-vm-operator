package v1alpha1

import "time"

// VMPhase defines the lifecycle stage of a Virtual Machine.
type VMPhase string

const (
	PhasePending      VMPhase = "Pending"
	PhaseValidating   VMPhase = "Validating"
	PhaseProvisioning VMPhase = "Provisioning"
	PhaseRunning      VMPhase = "Running"
	PhaseDegraded     VMPhase = "Degraded"
	PhaseStopped      VMPhase = "Stopped"
	PhaseFailed       VMPhase = "Failed"
	PhaseTerminating  VMPhase = "Terminating"
)

// RestartPolicy determines reconciler behavior when a VM process crashes or exits.
type RestartPolicy string

const (
	RestartPolicyAlways    RestartPolicy = "Always"
	RestartPolicyOnFailure RestartPolicy = "OnFailure"
	RestartPolicyNever     RestartPolicy = "Never"
)

// ConditionStatus represents True, False, or Unknown.
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// VMCondition describes the state of a VM at a certain point.
type VMCondition struct {
	Type               string          `json:"type" yaml:"type"`
	Status             ConditionStatus `json:"status" yaml:"status"`
	Reason             string          `json:"reason" yaml:"reason"`
	Message            string          `json:"message" yaml:"message"`
	LastTransitionTime time.Time       `json:"lastTransitionTime" yaml:"lastTransitionTime"`
}

// ImageSource defines the disk image source with air-gapped security parameters.
type ImageSource struct {
	// Name of the image in the local air-gapped registry.
	Name string `json:"name" yaml:"name"`
	// ChecksumSHA256 is the expected cryptographic hash for air-gap verification.
	ChecksumSHA256 string `json:"checksumSha256" yaml:"checksumSha256"`
	// RemoteURL is strictly blocked in air-gapped mode. If populated, validator rejects.
	RemoteURL string `json:"remoteUrl,omitempty" yaml:"remoteUrl,omitempty"`
}

// ResourceRequirements specifies compute resources.
type ResourceRequirements struct {
	VCPU     int `json:"vcpu" yaml:"vcpu"`
	MemoryMB int `json:"memoryMb" yaml:"memoryMb"`
	DiskGB   int `json:"diskGb" yaml:"diskGb"`
}

// NetworkConfig defines local network topology for the VM.
type NetworkConfig struct {
	Isolated     bool     `json:"isolated" yaml:"isolated"`
	InternalIP   string   `json:"internalIp,omitempty" yaml:"internalIp,omitempty"`
	AllowedPorts []int    `json:"allowedPorts,omitempty" yaml:"allowedPorts,omitempty"`
}

// VirtualMachineSpec is the desired specification of the VM.
type VirtualMachineSpec struct {
	Resources     ResourceRequirements `json:"resources" yaml:"resources"`
	Image         ImageSource          `json:"image" yaml:"image"`
	RestartPolicy RestartPolicy        `json:"restartPolicy" yaml:"restartPolicy"`
	Network       NetworkConfig        `json:"network" yaml:"network"`
}

// VirtualMachineStatus contains observed runtime state.
type VirtualMachineStatus struct {
	Phase              VMPhase       `json:"phase" yaml:"phase"`
	PID                int           `json:"pid,omitempty" yaml:"pid,omitempty"`
	AllocatedIP        string        `json:"allocatedIp,omitempty" yaml:"allocatedIp,omitempty"`
	RestartCount       int           `json:"restartCount" yaml:"restartCount"`
	Message            string        `json:"message,omitempty" yaml:"message,omitempty"`
	Conditions         []VMCondition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	LastTransitionTime time.Time     `json:"lastTransitionTime" yaml:"lastTransitionTime"`
}

// ObjectMeta provides metadata standard in Kubernetes resources.
type ObjectMeta struct {
	Name      string            `json:"name" yaml:"name"`
	Namespace string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	CreatedAt time.Time         `json:"createdAt" yaml:"createdAt"`
}

// VirtualMachine is the top-level Custom Resource Definition (CRD) struct.
type VirtualMachine struct {
	APIVersion string               `json:"apiVersion" yaml:"apiVersion"`
	Kind       string               `json:"kind" yaml:"kind"`
	Metadata   ObjectMeta           `json:"metadata" yaml:"metadata"`
	Spec       VirtualMachineSpec   `json:"spec" yaml:"spec"`
	Status     VirtualMachineStatus `json:"status" yaml:"status"`
}

// SetCondition helper updates or appends a condition.
func (vm *VirtualMachine) SetCondition(condType string, status ConditionStatus, reason, message string) {
	now := time.Now()
	for i, c := range vm.Status.Conditions {
		if c.Type == condType {
			vm.Status.Conditions[i] = VMCondition{
				Type:               condType,
				Status:             status,
				Reason:             reason,
				Message:            message,
				LastTransitionTime: now,
			}
			return
		}
	}
	vm.Status.Conditions = append(vm.Status.Conditions, VMCondition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
}


// DeepCopy returns an independent, thread-safe deep copy of the VirtualMachine.
func (vm *VirtualMachine) DeepCopy() *VirtualMachine {
	if vm == nil {
		return nil
	}

	clone := *vm

	// Deep copy Metadata Labels map
	if vm.Metadata.Labels != nil {
		clone.Metadata.Labels = make(map[string]string, len(vm.Metadata.Labels))
		for k, v := range vm.Metadata.Labels {
			clone.Metadata.Labels[k] = v
		}
	}

	// Deep copy Network AllowedPorts slice
	if vm.Spec.Network.AllowedPorts != nil {
		clone.Spec.Network.AllowedPorts = make([]int, len(vm.Spec.Network.AllowedPorts))
		copy(clone.Spec.Network.AllowedPorts, vm.Spec.Network.AllowedPorts)
	}

	// Deep copy Status Conditions slice
	if vm.Status.Conditions != nil {
		clone.Status.Conditions = make([]VMCondition, len(vm.Status.Conditions))
		copy(clone.Status.Conditions, vm.Status.Conditions)
	}

	return &clone
}
