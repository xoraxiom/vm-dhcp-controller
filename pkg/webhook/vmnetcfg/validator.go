package vmnetcfg

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	networkv1 "github.com/harvester/vm-dhcp-controller/pkg/apis/network.harvesterhci.io/v1alpha1"
	ctlcniv1 "github.com/harvester/vm-dhcp-controller/pkg/generated/controllers/k8s.cni.cncf.io/v1"
	ctlnetworkv1 "github.com/harvester/vm-dhcp-controller/pkg/generated/controllers/network.harvesterhci.io/v1alpha1"
	"github.com/harvester/vm-dhcp-controller/pkg/util"
	"github.com/harvester/vm-dhcp-controller/pkg/webhook"
	"github.com/harvester/webhook/pkg/server/admission"
	"github.com/sirupsen/logrus"
)

type Validator struct {
	admission.DefaultValidator

	nadCache    ctlcniv1.NetworkAttachmentDefinitionCache
	ippoolCache ctlnetworkv1.IPPoolCache
}

func NewValidator(nadCache ctlcniv1.NetworkAttachmentDefinitionCache, ippoolCache ctlnetworkv1.IPPoolCache) *Validator {
	return &Validator{
		nadCache:    nadCache,
		ippoolCache: ippoolCache,
	}
}

func (v *Validator) Create(request *admission.Request, newObj runtime.Object) error {
	vmNetCfg := newObj.(*networkv1.VirtualMachineNetworkConfig)
	logrus.Infof("create vmnetcfg %s/%s", vmNetCfg.Namespace, vmNetCfg.Name)

	for _, nc := range vmNetCfg.Spec.NetworkConfigs {
		// Use shared utility to look up IPPool via NAD labels
		// Uses vmNetCfg.Namespace as fallback for unqualified network names
		if _, err := util.GetIPPoolFromNetworkName(v.nadCache, v.ippoolCache, nc.NetworkName, vmNetCfg.Namespace); err != nil {
			return fmt.Errorf(webhook.CreateErr, vmNetCfg.Kind, vmNetCfg.Namespace, vmNetCfg.Name, err)
		}
	}

	return nil
}

func (v *Validator) Resource() admission.Resource {
	return admission.Resource{
		Names:      []string{"virtualmachinenetworkconfigs"},
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   networkv1.SchemeGroupVersion.Group,
		APIVersion: networkv1.SchemeGroupVersion.Version,
		ObjectType: &networkv1.VirtualMachineNetworkConfig{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}
