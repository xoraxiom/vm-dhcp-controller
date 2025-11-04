package vm

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	kubevirtv1 "kubevirt.io/api/core/v1"

	networkv1 "github.com/harvester/vm-dhcp-controller/pkg/apis/network.harvesterhci.io/v1alpha1"
	"github.com/harvester/vm-dhcp-controller/pkg/config"
	ctlkubevirtv1 "github.com/harvester/vm-dhcp-controller/pkg/generated/controllers/kubevirt.io/v1"
	ctlnetworkv1 "github.com/harvester/vm-dhcp-controller/pkg/generated/controllers/network.harvesterhci.io/v1alpha1"
)

const (
	controllerName = "vm-dhcp-vm-controller"

	vmLabelKey            = "harvesterhci.io/vmName"
	macAddressAnnotation  = "harvesterhci.io/mac-address"
)

type Handler struct {
	vmController   ctlkubevirtv1.VirtualMachineController
	vmClient       ctlkubevirtv1.VirtualMachineClient
	vmCache        ctlkubevirtv1.VirtualMachineCache
	vmnetcfgClient ctlnetworkv1.VirtualMachineNetworkConfigClient
	vmnetcfgCache  ctlnetworkv1.VirtualMachineNetworkConfigCache
}

func Register(ctx context.Context, management *config.Management) error {
	vms := management.KubeVirtFactory.Kubevirt().V1().VirtualMachine()
	vmnetcfgs := management.HarvesterNetworkFactory.Network().V1alpha1().VirtualMachineNetworkConfig()

	handler := &Handler{
		vmController:   vms,
		vmClient:       vms,
		vmCache:        vms.Cache(),
		vmnetcfgClient: vmnetcfgs,
		vmnetcfgCache:  vmnetcfgs.Cache(),
	}

	vms.OnChange(ctx, controllerName, handler.OnChange)

	return nil
}

func (h *Handler) OnChange(key string, vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error) {
	if vm == nil || vm.DeletionTimestamp != nil {
		return nil, nil
	}

	logrus.Debugf("(vm.OnChange) vm configuration %s/%s has been changed", vm.Namespace, vm.Name)

	// Apply MAC addresses from annotation to VM spec if missing
	vmCopy, updated, err := h.applyMACAddressAnnotation(vm)
	if err != nil {
		logrus.Errorf("(vm.OnChange) failed to apply MAC address annotation for vm %s: %v", key, err)
		return vm, err
	}

	// If we updated the VM spec, persist the changes
	if updated {
		logrus.Infof("(vm.OnChange) applied MAC addresses from annotation to vm %s", key)
		vm, err = h.vmClient.Update(vmCopy)
		if err != nil {
			return vm, err
		}
	}

	ncm := make(map[string]networkv1.NetworkConfig, 1)

	// Construct initial network config map
	for _, nic := range vm.Spec.Template.Spec.Domain.Devices.Interfaces {
		if nic.MacAddress == "" {
			continue
		}
		ncm[nic.Name] = networkv1.NetworkConfig{
			MACAddress: nic.MacAddress,
		}
	}

	// Update network name for each network config if it's of type Multus
	for _, network := range vm.Spec.Template.Spec.Networks {
		if network.Multus == nil {
			continue
		}
		nc, ok := ncm[network.Name]
		if !ok {
			continue
		}
		nc.NetworkName = network.Multus.NetworkName
		ncm[network.Name] = nc
	}

	// Remove incomplete network configs
	for i, nc := range ncm {
		if nc.NetworkName == "" {
			delete(ncm, i)
		}
	}

	// If no network config is found, return early
	if len(ncm) == 0 {
		logrus.Infof("(vm.OnChange) no effective network configs found for vm %s, skipping", key)
		return vm, nil
	}

	vmNetCfg := prepareVmNetCfg(vm, ncm)

	oldVmNetCfg, err := h.vmnetcfgCache.Get(vm.Namespace, vm.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("(vm.OnChange) create vmnetcfg for vm %s", key)
			if _, err := h.vmnetcfgClient.Create(vmNetCfg); err != nil {
				return vm, err
			}
			return vm, nil
		}
		return vm, err
	}

	logrus.Debugf("(vm.OnChange) vmnetcfg for vm %s already exists", key)

	vmNetCfgCpy := oldVmNetCfg.DeepCopy()
	vmNetCfgCpy.Spec.NetworkConfigs = vmNetCfg.Spec.NetworkConfigs

	// The following block is a two-step process. Ideally,
	// 1. if the network config of the VirtualMachine has been changed, update the status of the VirtualMachineNetworkConfig
	//   to out-of-sync so that the vmnetcfg-controller can handle it accordingly, and
	// 2. since the spec of the VirtualMachineNetworkConfig hasn't been changed, update it to reflect the new network config.
	// This is to throttle the vmnetcfg-controller and to avoid allocate-before-deallocate from happening.
	if !reflect.DeepEqual(vmNetCfgCpy.Spec.NetworkConfigs, oldVmNetCfg.Spec.NetworkConfigs) {
		if networkv1.InSynced.IsFalse(oldVmNetCfg) {
			logrus.Infof("(vm.OnChange) vmnetcfg %s/%s is deemed out-of-sync, updating it", vmNetCfgCpy.Namespace, vmNetCfgCpy.Name)
			if _, err := h.vmnetcfgClient.Update(vmNetCfgCpy); err != nil {
				return vm, err
			}
			return vm, nil
		}

		logrus.Infof("(vm.OnChange) update vmnetcfg %s/%s status as out-of-sync due to network config changes", vmNetCfgCpy.Namespace, vmNetCfgCpy.Name)

		// Mark the VirtualMachineNetworkConfig as out-of-sync so that the vmnetcfg-controller can handle it accordingly
		networkv1.InSynced.SetStatus(vmNetCfgCpy, string(corev1.ConditionFalse))
		networkv1.InSynced.Reason(vmNetCfgCpy, "NetworkConfigChanged")
		networkv1.InSynced.Message(vmNetCfgCpy, "Network configuration of the upstrem virtual machine has been changed")

		if _, err := h.vmnetcfgClient.UpdateStatus(vmNetCfgCpy); err != nil {
			return vm, err
		}

		// Enqueue the VirtualMachine in order to update the network config of its corresponding VirtualMachineNetworkConfig
		h.vmController.Enqueue(vm.Namespace, vm.Name)
	}

	return vm, nil
}

// applyMACAddressAnnotation applies MAC addresses from the annotation to VM interfaces that don't have MAC addresses set.
// It returns a deep copy of the VM with updated MAC addresses, a boolean indicating if any updates were made, and an error if any.
func (h *Handler) applyMACAddressAnnotation(vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, bool, error) {
	// Check if the annotation exists
	macAnnotation, exists := vm.Annotations[macAddressAnnotation]
	if !exists || macAnnotation == "" {
		return vm, false, nil
	}

	// Parse the annotation JSON: {"interface-name": "mac-address", ...}
	var macAddresses map[string]string
	if err := json.Unmarshal([]byte(macAnnotation), &macAddresses); err != nil {
		logrus.Warnf("(vm.applyMACAddressAnnotation) failed to parse MAC address annotation for vm %s/%s: %v", vm.Namespace, vm.Name, err)
		return vm, false, nil
	}

	if len(macAddresses) == 0 {
		return vm, false, nil
	}

	// Create a deep copy to avoid modifying the original
	vmCopy := vm.DeepCopy()
	updated := false

	// Apply MAC addresses to interfaces that don't have them set
	for i := range vmCopy.Spec.Template.Spec.Domain.Devices.Interfaces {
		nic := &vmCopy.Spec.Template.Spec.Domain.Devices.Interfaces[i]

		// Skip if MAC address is already set
		if nic.MacAddress != "" {
			continue
		}

		// Check if we have a MAC address for this interface in the annotation
		if macAddr, ok := macAddresses[nic.Name]; ok && macAddr != "" {
			logrus.Infof("(vm.applyMACAddressAnnotation) applying MAC address %s to interface %s on vm %s/%s", macAddr, nic.Name, vm.Namespace, vm.Name)
			nic.MacAddress = macAddr
			updated = true
		}
	}

	return vmCopy, updated, nil
}
