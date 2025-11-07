package util

import (
	"encoding/json"
	"fmt"
	"net"
	"net/netip"

	"github.com/rancher/wrangler/v3/pkg/kv"
	corev1 "k8s.io/api/core/v1"

	networkv1 "github.com/harvester/vm-dhcp-controller/pkg/apis/network.harvesterhci.io/v1alpha1"
	ctlcniv1 "github.com/harvester/vm-dhcp-controller/pkg/generated/controllers/k8s.cni.cncf.io/v1"
	ctlnetworkv1 "github.com/harvester/vm-dhcp-controller/pkg/generated/controllers/network.harvesterhci.io/v1alpha1"
)

type PoolInfo struct {
	IPNet           *net.IPNet
	NetworkIPAddr   netip.Addr
	BroadcastIPAddr netip.Addr
	StartIPAddr     netip.Addr
	EndIPAddr       netip.Addr
	ServerIPAddr    netip.Addr
	RouterIPAddr    netip.Addr
}

func GetServiceCIDRFromNode(node *corev1.Node) (string, error) {
	if node.Annotations == nil {
		return "", fmt.Errorf("service CIDR not found for node %s", node.Name)
	}

	nodeArgs, ok := node.Annotations[NodeArgsAnnotationKey]
	if !ok {
		return "", fmt.Errorf("annotation %s not found for node %s", NodeArgsAnnotationKey, node.Name)
	}

	var argList []string
	if err := json.Unmarshal([]byte(nodeArgs), &argList); err != nil {
		return "", err
	}

	var serviceCIDRIndex int
	for i, val := range argList {
		if val == ServiceCIDRFlag {
			// The "rke2.io/node-args" annotation in node objects contains various node arguments.
			// For example, '[...,"--cluster-cidr","10.52.0.0/16","--service-cidr","10.53.0.0/16", ...]'
			// What we need here is the value of the "--service-cidr" flag.
			// It could be accessed by accumulating the flag index by one.
			serviceCIDRIndex = i + 1
			break
		}
	}

	if serviceCIDRIndex == 0 || serviceCIDRIndex >= len(argList) {
		return "", fmt.Errorf("serviceCIDR not found for node %s", node.Name)
	}

	return argList[serviceCIDRIndex], nil
}

func LoadCIDR(cidr string) (ipNet *net.IPNet, networkIPAddr netip.Addr, broadcastIPAddr netip.Addr, err error) {
	_, ipNet, err = net.ParseCIDR(cidr)
	if err != nil {
		return
	}

	networkIPAddr, ok := netip.AddrFromSlice(ipNet.IP)
	if !ok {
		err = fmt.Errorf("cannot convert ip address %s", ipNet.IP)
		return
	}

	broadcastIP := make(net.IP, len(ipNet.IP))
	copy(broadcastIP, ipNet.IP)
	for i := range broadcastIP {
		broadcastIP[i] |= ^ipNet.Mask[i]
	}
	broadcastIPAddr, ok = netip.AddrFromSlice(broadcastIP)
	if !ok {
		err = fmt.Errorf("cannot convert ip address %s", broadcastIP)
		return
	}

	return
}

func LoadPool(ipPool *networkv1.IPPool) (pi PoolInfo, err error) {
	pi.IPNet, pi.NetworkIPAddr, pi.BroadcastIPAddr, err = LoadCIDR(ipPool.Spec.IPv4Config.CIDR)
	if err != nil {
		return
	}

	if ipPool.Spec.IPv4Config.Pool.Start != "" {
		pi.StartIPAddr, err = netip.ParseAddr(ipPool.Spec.IPv4Config.Pool.Start)
		if err != nil {
			return
		}
	}

	if ipPool.Spec.IPv4Config.Pool.End != "" {
		pi.EndIPAddr, err = netip.ParseAddr(ipPool.Spec.IPv4Config.Pool.End)
		if err != nil {
			return
		}
	}

	if ipPool.Spec.IPv4Config.ServerIP != "" {
		pi.ServerIPAddr, err = netip.ParseAddr(ipPool.Spec.IPv4Config.ServerIP)
		if err != nil {
			return
		}
	}

	if ipPool.Spec.IPv4Config.Router != "" {
		pi.RouterIPAddr, err = netip.ParseAddr(ipPool.Spec.IPv4Config.Router)
		if err != nil {
			return
		}
	}

	return
}

// LoadAllocated returns the un-allocatable IP addresses in three types of IP
// address lists, allocatedList, excludedList, and reservedList.
func LoadAllocated(allocated map[string]string) (allocatedList, excludedList, reservedList []netip.Addr) {
	for ip, val := range allocated {
		ipAddr, err := netip.ParseAddr(ip)
		if err != nil {
			continue
		}

		switch val {
		case ExcludedMark:
			excludedList = append(excludedList, ipAddr)
		case ReservedMark:
			reservedList = append(reservedList, ipAddr)
		default:
			allocatedList = append(allocatedList, ipAddr)
		}
	}
	return
}

func IsIPAddrInList(ipAddr netip.Addr, ipAddrList []netip.Addr) bool {
	for i := range ipAddrList {
		if ipAddr == ipAddrList[i] {
			return true
		}
	}
	return false
}

func IsIPInBetweenOf(ip, ip1, ip2 string) bool {
	ipAddr, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	ip1Addr, err := netip.ParseAddr(ip1)
	if err != nil {
		return false
	}
	ip2Addr, err := netip.ParseAddr(ip2)
	if err != nil {
		return false
	}

	return ipAddr.Compare(ip1Addr) >= 0 && ipAddr.Compare(ip2Addr) <= 0
}

// GetIPPoolFromNetworkName resolves an IPPool from a network name by:
// 1. Looking up the NetworkAttachmentDefinition
// 2. Reading IPPool namespace/name from NAD labels
// 3. Retrieving the IPPool resource
//
// If networkName doesn't include a namespace prefix (e.g., "my-network" vs "default/my-network"),
// it defaults to the provided fallbackNamespace. Pass an empty string to fallbackNamespace
// to use no default (namespace will be empty if not specified in networkName).
//
// This function provides a single source of truth for IPPool lookup logic, preventing
// duplication across controllers and webhooks.
//
// Concurrency note: This function performs multiple cache lookups (NAD, then IPPool) without
// atomicity. In rare cases, a resource could be deleted between lookups. This is acceptable
// because:
// - Caches are eventually consistent with the Kubernetes API server
// - Controllers will re-reconcile on the next event (watch/resync)
// - The Kubernetes controller pattern expects and handles transient inconsistencies
func GetIPPoolFromNetworkName(
	nadCache ctlcniv1.NetworkAttachmentDefinitionCache,
	ippoolCache ctlnetworkv1.IPPoolCache,
	networkName string,
	fallbackNamespace string,
) (*networkv1.IPPool, error) {
	nadNamespace, nadName := kv.RSplit(networkName, "/")
	if nadNamespace == "" {
		nadNamespace = fallbackNamespace
	}

	nad, err := nadCache.Get(nadNamespace, nadName)
	if err != nil {
		return nil, fmt.Errorf("network attachment definition %s/%s not found: %w", nadNamespace, nadName, err)
	}

	if nad.Labels == nil {
		return nil, fmt.Errorf("network attachment definition %s/%s has no labels", nadNamespace, nadName)
	}

	ipPoolNamespace, ok := nad.Labels[IPPoolNamespaceLabelKey]
	if !ok {
		return nil, fmt.Errorf("network attachment definition %s/%s has no label %s", nadNamespace, nadName, IPPoolNamespaceLabelKey)
	}

	ipPoolName, ok := nad.Labels[IPPoolNameLabelKey]
	if !ok {
		return nil, fmt.Errorf("network attachment definition %s/%s has no label %s", nadNamespace, nadName, IPPoolNameLabelKey)
	}

	ipPool, err := ippoolCache.Get(ipPoolNamespace, ipPoolName)
	if err != nil {
		return nil, fmt.Errorf("ippool %s/%s not found: %w", ipPoolNamespace, ipPoolName, err)
	}

	return ipPool, nil
}
