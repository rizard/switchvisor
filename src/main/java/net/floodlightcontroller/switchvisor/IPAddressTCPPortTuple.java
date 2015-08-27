package net.floodlightcontroller.switchvisor;

import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.TransportPort;

public class IPAddressTCPPortTuple {
	private IPv4Address ip;
	private TransportPort tcp;
	
	private IPAddressTCPPortTuple(IPv4Address ip, TransportPort tcp) {
		this.ip = ip;
		this.tcp = tcp;
	}
	
	public static IPAddressTCPPortTuple of(IPv4Address ip, TransportPort tcp) {
		if (ip == null) {
			throw new NullPointerException("IPv4Address must not be null.");
		}
		if (ip == IPv4Address.NONE || ip == IPv4Address.NO_MASK) {
			throw new IllegalArgumentException("IPv4Address must be a valid IPv4Address (non-zero and not fully-masked).");
		}
		if (tcp == null) {
			throw new NullPointerException("TransportPort must not be null.");
		}
		if (tcp == TransportPort.NONE) {
			throw new IllegalArgumentException("TransportPort must be a valid TransportPort value (non-zero).");
		}
		
		return new IPAddressTCPPortTuple(ip, tcp);
	}
	
	public IPv4Address getIPAddress() {
		return this.ip;
	}
	
	public TransportPort getTCPPort() {
		return this.tcp;
	}
}
