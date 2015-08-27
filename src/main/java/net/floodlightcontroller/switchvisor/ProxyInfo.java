package net.floodlightcontroller.switchvisor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TransportPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * Contains the information provided by the RCA necessary to
 * establish and maintain a mapping between a switch, the proxy,
 * and the user's controller. Specifically:
 * -- the physical switch DPID
 * -- the physical switch ports
 * -- the proxy switch DPID
 * -- the proxy switch ports
 * -- the user's controller IP
 * -- the user's controller TCP port
 * 
 * Under the hood, a Google BiMap is used to provide efficient access
 * to the port mappings from either key-to-value or value-to-key.
 * 
 * @author Ryan Izard, ryan.izard@bigswitch.com, rizard@g.clemson.edu
 *
 */
public class ProxyInfo {
	private static final Logger log = LoggerFactory.getLogger(ProxyInfo.class);

	private volatile DatapathId physicalDpid; /* not final, since this might/should change upon migration */
	private volatile OFVersion physicalVersion; /* mutable */
	private final DatapathId proxyDpid;
	private volatile OFVersion proxyVersion; /* mutable */
	private final BiMap<OFPort, OFPort> portMap; /* use a BiMap for quicker access to data */
	private final IPv4Address controllerIp;
	private final TransportPort controllerPort;

	/**
	 * Create a new ProxyInfo, which is used to house information
	 * relative to the current physical proxy for a user's slice.
	 * 
	 * Custom type UniqueElementList is used to ensure order of
	 * OpenFlow ports is preserved and that they are unique.
	 * 
	 * The mapping from physical-to-proxy port will be the order of
	 * the ports in the lists, such that:
	 *   physicalPorts[0] <--> proxyPorts[0]
	 *   physicalPorts[1] <--> proxyPorts[1]
	 *           ...      <-->     ...
	 *   physicalPorts[i] <--> proxyPorts[i]
	 * 
	 * @param physicalDpid
	 * @param physicalPorts
	 * @param proxyDpid, immutable after creation
	 * @param proxyPorts, immutable after creation
	 * @param controllerIp, immutable after creation
	 * @param controllerPort, immutable after creation
	 */
	public static ProxyInfo of(@Nonnull DatapathId physicalDpid, @Nonnull OrderedSet<OFPort> physicalPorts,
			@Nonnull DatapathId proxyDpid, @Nonnull OrderedSet<OFPort> proxyPorts,
			@Nonnull IPv4Address controllerIp, @Nonnull TransportPort controllerTcpPort) {
		if (physicalDpid == null) {
			throw new NullPointerException("Physical DPID cannot be null.");
		}
		if (physicalPorts == null) {
			throw new NullPointerException("Physical ports cannot be null.");
		}
		if (proxyDpid == null) {
			throw new NullPointerException("Proxy DPID cannot be null.");
		}
		if (proxyPorts == null) {
			throw new NullPointerException("Proxy ports cannot be null.");
		}
		if (controllerIp == null) {
			throw new NullPointerException("Controller IP cannot be null.");
		}
		if (controllerTcpPort == null) {
			throw new NullPointerException("Controller TCP port cannot be null.");
		}

		/*
		 * Abort if the ports provided cannot form a one-to-one mapping.
		 */
		if (physicalPorts.size() != proxyPorts.size()) {
			throw new IllegalArgumentException("Physical ports and proxy ports must be equal in quantity / must form a 1-1 mapping, phys[i]:proxy[i].");
		} else {
			return new ProxyInfo(physicalDpid, physicalPorts, proxyDpid, proxyPorts, controllerIp, controllerTcpPort);
		}
	}

	/**
	 * Create a new ProxyInfo, which is used to house information
	 * relative to the current physical proxy for a user's slice.
	 * 
	 * Custom type UniqueElementList is used to ensure order of
	 * OpenFlow ports is preserved and that they are unique.
	 * 
	 * The mapping from physical-to-proxy port will be the order of
	 * the ports in the lists, such that:
	 *   physicalPorts[0] <-->  1
	 *   physicalPorts[1] <-->  2
	 *           ...      <--> ...
	 *   physicalPorts[i] <-->  i
	 * 
	 * @param physicalDpid
	 * @param physicalPorts
	 * @param proxyDpid, immutable after creation
	 * @param controllerIp, immutable after creation
	 * @param controllerPort, immutable after creation
	 */
	public static ProxyInfo of(@Nonnull DatapathId physicalDpid, @Nonnull OrderedSet<OFPort> physicalPorts,
			@Nonnull DatapathId proxyDpid, @Nonnull IPv4Address controllerIp, @Nonnull TransportPort controllerTcpPort) {
		if (physicalDpid == null) {
			throw new NullPointerException("Physical DPID cannot be null.");
		}
		if (physicalPorts == null) {
			throw new NullPointerException("Physical ports cannot be null.");
		}
		if (proxyDpid == null) {
			throw new NullPointerException("Proxy DPID cannot be null.");
		}
		if (controllerIp == null) {
			throw new NullPointerException("Controller IP cannot be null.");
		}
		if (controllerTcpPort == null) {
			throw new NullPointerException("Controller TCP port cannot be null.");
		}

		OrderedSet<OFPort> proxyPorts = new OrderedSet<OFPort>(physicalPorts.size());
		for (int i = 0; i < physicalPorts.size(); i++) {
			proxyPorts.add(OFPort.of(i));
			i++;
		}
		return new ProxyInfo(physicalDpid, physicalPorts, proxyDpid, proxyPorts, controllerIp, controllerTcpPort);
	}

	private ProxyInfo(DatapathId physicalDpid, OrderedSet<OFPort> physicalPorts,
			DatapathId proxyDpid, OrderedSet<OFPort> proxyPorts,
			IPv4Address controllerIp, TransportPort controllerPort) {
		this.physicalDpid = physicalDpid;
		this.proxyDpid = proxyDpid;		
		this.controllerIp = controllerIp;
		this.controllerPort = controllerPort;
		this.portMap = HashBiMap.create();


		Iterator<OFPort> realItr = physicalPorts.iterator();
		Iterator<OFPort> fakeItr = proxyPorts.iterator();
		while (realItr.hasNext() && fakeItr.hasNext()) {
			this.portMap.put(realItr.next(), fakeItr.next());
		}
		log.debug("Created new ProxyInfo {}");
	}

	public DatapathId getPhysicalSwitchDpid() {
		return this.physicalDpid;
	}

	public synchronized void setPhysicalSwitchDPID(DatapathId dpid) {
		this.physicalDpid = dpid;
	}

	public DatapathId getProxySwitchDpid() {
		return this.proxyDpid;
	}

	/**
	 * Use to lookup the proxy port of a bound physical port.
	 * 
	 * @param physicalPort
	 * @return
	 */
	public OFPort getProxyPortFromPhysical(OFPort physicalPort) {
		return this.portMap.containsKey(physicalPort) ? this.portMap.get(physicalPort) : physicalPort;
	}

	public List<OFPort> getProxyPortFromPhysical(List<OFPort> ports) {
		List<OFPort> r = new ArrayList<OFPort>(ports.size());
		for (OFPort p : ports) {
			r.add(getProxyPortFromPhysical(p));
		}
		return r;
	}

	public OFPortDesc getProxyPortDescFromPhysical(OFPortDesc p) {
		OFPortDesc.Builder nb = OFFactories.getFactory(this.proxyVersion).buildPortDesc()
				.setPortNo(getProxyPortFromPhysical(p.getPortNo()))
				.setHwAddr(p.getHwAddr())
				.setName(getProxyPortFromPhysical(p.getPortNo()).toString()) /* name will be the port number as string; might not match chassis, but it's the best we can do */
				.setConfig(p.getConfig())
				.setState(p.getState())
				.setCurr(p.getCurr())
				.setAdvertised(p.getAdvertised())
				.setSupported(p.getSupported())
				.setPeer(p.getPeer());
		if (this.proxyVersion.compareTo(OFVersion.OF_10) > 0 && this.physicalVersion.compareTo(OFVersion.OF_10) > 0) {
			nb.setCurrSpeed(p.getCurrSpeed())
			.setMaxSpeed(p.getMaxSpeed());
		}
		return nb.build();
	}
	
	public List<OFPortDesc> getProxyPortDescFromPhysical(List<OFPortDesc> ports) {
		List<OFPortDesc> r = new ArrayList<OFPortDesc>(ports.size());
		for (OFPortDesc p : ports) {
			r.add(getProxyPortDescFromPhysical(p));
		}
		return r;
	}
	
	public OFPortDesc getPhysicalPortDescFromProxy(OFPortDesc p) {
		OFPortDesc.Builder nb = OFFactories.getFactory(this.physicalVersion).buildPortDesc()
				.setPortNo(getPhysicalPortFromProxy(p.getPortNo()))
				.setHwAddr(p.getHwAddr())
				.setName(getPhysicalPortFromProxy(p.getPortNo()).toString()) /* name will be the port number as string; might not match chassis, but it's the best we can do */
				.setConfig(p.getConfig())
				.setState(p.getState())
				.setCurr(p.getCurr())
				.setAdvertised(p.getAdvertised())
				.setSupported(p.getSupported())
				.setPeer(p.getPeer());
		if (this.physicalVersion.compareTo(OFVersion.OF_10) > 0 && this.physicalVersion.compareTo(OFVersion.OF_10) > 0) {
			nb.setCurrSpeed(p.getCurrSpeed())
			.setMaxSpeed(p.getMaxSpeed());
		}
		return nb.build();
	}
	
	public List<OFPortDesc> getPhysicalPortDescFromProxy(List<OFPortDesc> ports) {
		List<OFPortDesc> r = new ArrayList<OFPortDesc>(ports.size());
		for (OFPortDesc p : ports) {
			r.add(getPhysicalPortDescFromProxy(p));
		}
		return r;
	}

	public OFPort getPhysicalPortFromProxy(OFPort proxyPort) {
		return this.portMap.inverse().containsKey(proxyPort) ? this.portMap.inverse().get(proxyPort) : proxyPort;
	}

	public List<OFPort> getPhysicalPortFromProxy(List<OFPort> ports) {
		List<OFPort> r = new ArrayList<OFPort>(ports.size());
		for (OFPort p : ports) {
			r.add(getPhysicalPortFromProxy(p));
		}
		return r;
	}

	public IPv4Address getControllerIp() {
		return this.controllerIp;
	}

	public TransportPort getControllerTCPPort() {
		return this.controllerPort;
	}

	public synchronized void setPhysicalSwitchVersion(OFVersion v) {
		this.physicalVersion = v;
	}

	public OFVersion getPhysicalSwitchVersion() {
		if (this.physicalVersion == null) {
			throw new NullPointerException("Cannot access physical switch version without first setting it.");
		}
		return this.physicalVersion;
	}

	public OFVersion getProxySwitchVersion() {
		if (this.proxyVersion == null) {
			throw new NullPointerException("Cannot access proxy switch version without first setting it.");
		}
		return this.proxyVersion;
	}

	/**
	 * Replace the physical switch DPID and ports with a new set
	 * (i.e. after a migration). This will lock on the ProxyInfo
	 * object in order to make sure nobody else tries to access
	 * the list.
	 * 
	 * @param newDpid
	 * @param newPhysPorts
	 */
	public synchronized void replacePhysical(@Nonnull DatapathId newDpid, @Nonnull OrderedSet<OFPort> newPhysPorts) {
		if (newDpid == null) {
			throw new NullPointerException("New DPID cannot be null.");
		}
		if (newPhysPorts == null) {
			throw new NullPointerException("New list of OFPorts cannot be null.");
		}
		if (newPhysPorts.size() != this.portMap.size()) {
			throw new IllegalArgumentException("Physical ports and proxy ports must be equal in quantity / must form a 1-1 mapping, phys[i]:proxy[i].");
		}

		this.physicalDpid = newDpid;
		Iterator<OFPort> proxyItr = this.portMap.values().iterator();
		Iterator<OFPort> physItr = newPhysPorts.iterator();
		while (proxyItr.hasNext() && physItr.hasNext()) {
			this.portMap.inverse().forcePut(proxyItr.next(), physItr.next()); // overwrite the prior physical-to-proxy port mapping
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((controllerIp == null) ? 0 : controllerIp.hashCode());
		result = prime * result
				+ ((controllerPort == null) ? 0 : controllerPort.hashCode());
		result = prime * result
				+ ((physicalDpid == null) ? 0 : physicalDpid.hashCode());
		result = prime * result + ((portMap == null) ? 0 : portMap.hashCode());
		result = prime * result
				+ ((proxyDpid == null) ? 0 : proxyDpid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProxyInfo other = (ProxyInfo) obj;
		if (controllerIp == null) {
			if (other.controllerIp != null)
				return false;
		} else if (!controllerIp.equals(other.controllerIp))
			return false;
		if (controllerPort == null) {
			if (other.controllerPort != null)
				return false;
		} else if (!controllerPort.equals(other.controllerPort))
			return false;
		if (physicalDpid == null) {
			if (other.physicalDpid != null)
				return false;
		} else if (!physicalDpid.equals(other.physicalDpid))
			return false;
		if (portMap == null) {
			if (other.portMap != null)
				return false;
		} else if (!portMap.keySet().equals(other.portMap.keySet())) //TODO
			return false;
		if (proxyDpid == null) {
			if (other.proxyDpid != null)
				return false;
		} else if (!proxyDpid.equals(other.proxyDpid))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ProxyInfo [physicalDpid=" + physicalDpid + ", physicalVersion="
				+ physicalVersion + ", proxyDpid=" + proxyDpid
				+ ", proxyVersion=" + proxyVersion + ", portMap=" + portMap
				+ ", controllerIp=" + controllerIp + ", controllerPort="
				+ controllerPort + "]";
	}
}