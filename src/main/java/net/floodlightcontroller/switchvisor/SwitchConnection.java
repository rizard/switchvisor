package net.floodlightcontroller.switchvisor;

import javax.annotation.Nonnull;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.TransportPort;

public class SwitchConnection {
	private final DatapathId dpid;
	private final IPv4Address ip;
	private final OrderedSet<TransportPort> sockets;
	
	private SwitchConnection(DatapathId dpid, IPv4Address ip, TransportPort mainSocket) {
		this.dpid = dpid;
		this.ip = ip;
		this.sockets = new OrderedSet<TransportPort>(1);
		this.sockets.add(mainSocket);
	}
	
	public static SwitchConnection of(@Nonnull DatapathId dpid, @Nonnull IPv4Address ip, @Nonnull TransportPort mainSocket) {
		if (dpid == null) {
			throw new NullPointerException("DatapathId must not be null.");
		}
		if (dpid == DatapathId.NONE) {
			throw new IllegalArgumentException("DatapathId must be a valid DPID (non-zero).");
		}
		if (ip == null) {
			throw new NullPointerException("IPv4Address must not be null.");
		}
		if (ip == IPv4Address.NONE || ip == IPv4Address.NO_MASK) {
			throw new IllegalArgumentException("IPv4Address must be a valid IPv4Address (non-zero and not fully-masked).");
		}
		if (mainSocket == null) {
			throw new NullPointerException("TransportPort must not be null.");
		}
		if (mainSocket == TransportPort.NONE) {
			throw new IllegalArgumentException("TransportPort must be a valid TransportPort value (non-zero).");
		}
		
		return new SwitchConnection(dpid, ip, mainSocket);
	}
	
	public boolean removeConnection(TransportPort tcpPort) {
		if (tcpPort == null) {
			throw new NullPointerException("TransportPort must not be null.");
		}
		return this.sockets.remove(tcpPort);
	}
	
	public boolean addConnection(TransportPort tcpPort) {
		if (tcpPort == null) {
			throw new NullPointerException("TransportPort must not be null.");
		}
		return this.sockets.add(tcpPort);
	}
	
	public DatapathId getDpid() {
		return this.dpid;
	}
	
	public IPv4Address getIPAddress() {
		return this.ip;
	}
	
	/**
	 * Checks if the given IP address with TCP port belong to this switch.
	 * 
	 * @param ip
	 * @param tcpPort
	 * @return
	 */
	public boolean isOurConnection(IPv4Address ip, TransportPort tcpPort) {
		if (!isOurIPAddress(ip)) {
			return false;
		} else if (!this.sockets.contains(tcpPort)) {
			return false;
		} else {
			return true;
		}
	}
	
	/**
	 * Checks if the given TCP port belongs to this switch. The IP address
	 * is assumed to be this switch's IP address. 
	 * 
	 * @param tcpPort
	 * @return
	 */
	public boolean isOurTCPPort(TransportPort tcpPort) {
		return isOurConnection(this.ip, tcpPort);
	}
	
	/**
	 * Checks if the given IPv4 address belongs to this switch.
	 * 
	 * @param ip
	 * @return
	 */
	public boolean isOurIPAddress(IPv4Address ip) {
		return this.ip.equals(ip);
	}
}
