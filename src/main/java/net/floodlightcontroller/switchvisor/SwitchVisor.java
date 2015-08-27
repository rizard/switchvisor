 package net.floodlightcontroller.switchvisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.projectfloodlight.openflow.exceptions.OFParseError;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFeaturesReply;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFMessageReader;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TransportPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.packet.Data;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.switchvisor.web.SwitchVisorRoutable;

public class SwitchVisor implements IFloodlightModule, IOFMessageListener, ISwitchVisorService, IOFSwitchListener {

	private static final Logger log = LoggerFactory.getLogger(SwitchVisor.class);

	private static IFloodlightProviderService floodlightProviderService;
	private static IRestApiService restApiService;
	private static IOFSwitchService switchService;

	private static int maxOfConnectionsSupported = 1024;
	private static int proxySessionTimeout = 60;

	private static List<DatapathId> intercepterSwitches;

	/*
	 * Keep track of the active proxies we should service.
	 * This is modified only by commands received from the
	 * REST API, e.g. add, delete, migrate, etc.
	 */
	private static OrderedSet<ProxyInfo> proxiesToService;

	@Override
	public String getName() {
		return SwitchVisor.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		if (type.equals(OFType.PACKET_IN) && name.equals("devicemanager")) {
			log.debug("Telling DeviceManager to run before us.");
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		if (type.equals(OFType.PACKET_IN) && (name.equals("forwarding") || name.equals("hub"))) {
			log.debug("Telling Forwarding/Hub to run before us.");
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		List<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(ISwitchVisorService.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		m.put(ISwitchVisorService.class, this);
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		List<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IRestApiService.class);
		l.add(IOFSwitchService.class);
		l.add(IDeviceService.class);
		l.add(IFloodlightProviderService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
		restApiService = context.getServiceImpl(IRestApiService.class);
		switchService = context.getServiceImpl(IOFSwitchService.class);

		intercepterSwitches = new ArrayList<DatapathId>();
		intercepterSwitches.add(DatapathId.of("00:00:00:00:00:00:00:11")); //TODO get from config/REST API

		proxiesToService = new OrderedSet<ProxyInfo>();

		Map<String, String> config = context.getConfigParams(this);
		if (config.get("session-timeout") != null && !config.get("session-timeout").isEmpty()) {
			proxySessionTimeout = Integer.parseInt(config.get("session-timeout"));
		}
		if (config.get("max-proxy-connections") != null && !config.get("max-proxy-connections").isEmpty()) {
			proxySessionTimeout = Integer.parseInt(config.get("max-proxy-connections"));
		}
		
		OrderedSet<OFPort> physPorts = new OrderedSet<OFPort>();
		physPorts.add(OFPort.of(1));
		physPorts.add(OFPort.of(2));
		physPorts.add(OFPort.of(3));
		OrderedSet<OFPort> proxyPorts = new OrderedSet<OFPort>();
		proxyPorts.add(OFPort.of(4));
		proxyPorts.add(OFPort.of(5));
		proxyPorts.add(OFPort.of(6));
		
		proxiesToService.add(ProxyInfo.of(DatapathId.of(1), physPorts, DatapathId.of(2), proxyPorts, IPv4Address.of("10.0.0.1"), TransportPort.of(6653)));
		log.warn("Manually added ProxyInfo {} for testing.", proxiesToService.get(0));
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProviderService.addOFMessageListener(OFType.PACKET_IN, this);
		switchService.addOFSwitchListener(this);
		restApiService.addRestletRoutable(new SwitchVisorRoutable());
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		/*
		 * First, verify the switch that sent us the packet is in fact
		 * a switch we're using to intercept our control connections.
		 */
		if (!isIntercepterSwitch(sw.getId())) {
			return Command.CONTINUE;
		}

		/*
		 * If the packet is not an IP, TCP, and packet that we need to
		 * perform a proxy for, then ignore it.
		 */
		Ethernet l2 = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		if (l2.getEtherType().equals(EthType.IPv4)) {
			IPv4 l3 = (IPv4) l2.getPayload();
			log.debug("Received an IPv4 packet. Checking if it's a TCP packet.");
			if (l3.getProtocol().equals(IpProtocol.TCP)) {
				TCP l4 = (TCP) l3.getPayload();
				/*
				 * Let the packet go if it's not a member of an active proxy session.
				 */
				if (!isPacketMemberOfProxySession(l3.getSourceAddress(), l4.getSourcePort(), l3.getDestinationAddress(), l4.getDestinationPort())) {
					log.debug("Not member of proxy: TCP packet {}", l3);
					return Command.CONTINUE;
				}

				ProxyInfo pi = getProxyInfo(l3.getSourceAddress(), l4.getSourcePort(), l3.getDestinationAddress(), l4.getDestinationPort());

				PacketDirection pd;
				try {
					pd = getPacketDirection(pi, l3.getSourceAddress(), l4.getSourcePort(), l3.getDestinationAddress(), l4.getDestinationPort());
				} catch (Exception e) {
					log.error("Error looking up packet direction. Report SwitchVisor bug.");
					log.error("Error message: {}", e.getMessage());
					e.printStackTrace();
					return Command.STOP;
				}
				/*
				 * If it is a member of a proxy, don't try to modify it if it's just
				 * an acknowledgement. A TCP ACK will have no payload. If an ACK does
				 * have payload, then it's OpenFlow "response" data that might need
				 * to be modified. So, only if the ACK data is non-zero in length, 
				 * ignore it.
				 *
				 * We could check the ACK flag, but it might include some data.
				 * So, just checking the TCP payload length should suffice.
				 */
				Data l7 = (Data) l4.getPayload();
				if (l7.getData().length > 0) {
					/* 
					 * Now, for the "hard part," deserialize the OpenFlow data using
					 * the in-built reader/writer in Loxi.
					 */
					List<OFMessage> ofMsgs = readOpenFlowMessages(l7.getData());
					List<OFMessage> mangledOfMsgs = new ArrayList<OFMessage>(ofMsgs.size());
					for (OFMessage m : ofMsgs) {
						/* First, process features reply, if present */
						if (m.getType().equals(OFType.FEATURES_REPLY)) {
							processFeaturesReply((OFFeaturesReply) m, pi);
							log.debug("Learned physical DPID and version in ProxyInfo {}", pi.toString());
						}
						/* Then, mangle packets (note includes features reply) */
						mangledOfMsgs.add(OFPacketMangler.mangle(m, pi, pd));
					}

					/*
					 * And, last but not least, let's take the messages and put them
					 * back into the TCP packet's payload once again. Use of lists
					 * guarantees order is correct.
					 */
					l7.setData(writeOpenFlowMessages(mangledOfMsgs));
					/*
					 * All layer payload references still set, so just serialize.
					 * New data set in application layer above will be incorporated.
					 */
					sendPacketOut(sw, IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_DST_DEVICE), l2, pi, pd);
					return Command.STOP;
				} else {
					log.debug("Received a proxy TCP packet with zero payload. Forwarding as-is.");
					sendPacketOut(sw, IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_DST_DEVICE), l2, pi, pd);
				}
			}						
		} else {
			log.debug("Not a TCP packet. Allowing other modules to handle it."); 
			return Command.CONTINUE;
		}

		return Command.STOP;
	}

	/*
	 * Helper functions
	 */

	/**
	 * Set the correct OpenFlow version and DPID from a features reply.
	 * @param m
	 * @param pi
	 */
	private static void processFeaturesReply(OFFeaturesReply m, ProxyInfo pi) {
		pi.setPhysicalSwitchVersion(m.getVersion());
		pi.setPhysicalSwitchDPID(m.getDatapathId());
	}
	
	/**
	 * Based on a valid ProxyInfo object, take the source and destination L3 and L4
	 * header information of a packet and lookup the direction of the packet, either
	 * going from the controller to the switch or from the switch to the controller.
	 * If the header information provided does not match that of the ProxyInfo, an
	 * exception is thrown. This helper function should only be used if the header
	 * information has already been verified a match to the ProxyInfo. To do this, see
	 * {@link SwitchVisor#getProxyInfo(IPv4Address, TransportPort, IPv4Address, TransportPort)}
	 * and
	 * {@link SwitchVisor#isPacketMemberOfProxySession(IPv4Address, TransportPort, IPv4Address, TransportPort)}
	 * 
	 * @param pi
	 * @param srcIp
	 * @param srcPort
	 * @param dstIp
	 * @param dstPort
	 * @return
	 * @throws Exception
	 */
	private PacketDirection getPacketDirection(ProxyInfo pi, IPv4Address srcIp, TransportPort srcPort, IPv4Address dstIp, TransportPort dstPort) throws Exception {
		if (pi.getControllerIp().equals(srcIp) && pi.getControllerTCPPort().equals(srcPort)) {
			return PacketDirection.PROXY_TO_PHYS;
		} else if (pi.getControllerIp().equals(dstIp) && pi.getControllerTCPPort().equals(dstPort)) {
			return PacketDirection.PHYS_TO_PROXY;
		} else {
			throw new Exception("Either src/dst of packet header [srcIp=" + srcIp.toString() + ",srcPort=" + srcPort.toString() + 
					"] [dstIp=" + dstIp.toString() + ",dstPort=" + dstPort.toString() + "] must belong to ProxyInfo " + pi.toString());
		}
	}

	/**
	 * Check if the IP and TCP header information from a packet matches a
	 * configured proxy. The caller should adhere to correct parameter ordering.
	 * Note that the packet's source IP and TCP port come first, followed by 
	 * the packet's destination IP and TCP port.
	 * 
	 * @param srcIp
	 * @param srcPort
	 * @param dstIp
	 * @param dstPort
	 * @return
	 */
	private boolean isPacketMemberOfProxySession(IPv4Address srcIp, TransportPort srcPort, IPv4Address dstIp, TransportPort dstPort) {
		return getProxyInfo(srcIp, srcPort, dstIp, dstPort) != null ? true : false;
	}

	/**
	 * Check if the IP and TCP header information from a packet matches a
	 * configured proxy. If a match is found, then the ProxyInfo for that
	 * proxy will be returned. The caller should adhere to correct parameter 
	 * ordering. Note that the packet's source IP and TCP port come first, 
	 * followed by the packet's destination IP and TCP port.
	 * 
	 * @param srcIp
	 * @param srcPort
	 * @param dstIp
	 * @param dstPort
	 * @return
	 */
	private ProxyInfo getProxyInfo(IPv4Address srcIp, TransportPort srcPort, IPv4Address dstIp, TransportPort dstPort) {
		for (ProxyInfo pi : proxiesToService) {
			if (pi.getControllerIp().equals(srcIp) && pi.getControllerTCPPort().equals(srcPort)) {
				/* Direction is from controller to switch */
				return pi;
			} else if (pi.getControllerIp().equals(dstIp) && pi.getControllerTCPPort().equals(dstPort)) {
				/* Direction is from switch to controller */
				return pi;
			}
		}
		return null;
	}

	/**
	 * Determine if the switch in question is supposed to give us
	 * OpenFlow packets for rewrite.
	 * 
	 * @param dpid, the switch that sent us the packet-in
	 * @return true if switch is registered as an intercepter; false otherwise
	 */
	private boolean isIntercepterSwitch(DatapathId dpid) {
		return intercepterSwitches.contains(dpid);
	}

	/**
	 * Get a list of OFMessages from a raw byte[] of OpenFlow data. The
	 * OpenFlow version of the message will automatically be determined
	 * during the parse.
	 * 
	 * @param data, the raw data to parse into OFMessages
	 * @return a list of OFMessages
	 */
	private static List<OFMessage> readOpenFlowMessages(byte[] data) {
		OFMessageReader<OFMessage> reader = OFFactories.getGenericReader();
		ChannelBuffer cb = ChannelBuffers.wrappedBuffer(data);

		List<OFMessage> ofMsgList = new ArrayList<OFMessage>();
		while (true) {
			OFMessage m = null;
			try {
				m = reader.readFrom(cb);
			} catch (OFParseError e) {
				log.error("Error parsing OpenFlow message. Details: {}", e);
			}
			if (m == null)
				break;
			ofMsgList.add(m);
		}
		return ofMsgList;
	}

	/**
	 * Get a byte[] from a list of OFMessages. The byte[] returned 
	 * conforms to the formats specified for each message in the 
	 * OpenFlow protocol. One such use for this function would be to
	 * serialize OpenFlow messages for inclusion in a TCP packet to
	 * a controller/switch.
	 *
	 * @param msgs, the list of OpenFlow messages to convert to byte form
	 * @return the raw byte[] representing the OpenFlow messages
	 */
	private static byte[] writeOpenFlowMessages(List<OFMessage> msgs) {
		ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
		for (OFMessage ofMsg : msgs) {
			ofMsg.writeTo(cb);
		}
		return cb.array();
	}

	/**
	 * Take a packet with all payloads/data already set, and then
	 * send it as a packet out to the appropriate device (i.e. 
	 * switch or controller) as indicated by the ProxyInfo and 
	 * PacketDirection.
	 * 
	 * @param sw, the switch that will conduct the packet-out
	 * @param eth, the packet to serialize and write
	 * @param pi, information about the proxy
	 * @param pd, the direction the packet should be sent
	 */
	private static void sendPacketOut(IOFSwitch sw, IDevice dstDevice, Ethernet eth, ProxyInfo pi, PacketDirection pd) {
		/*
		 * Determine where to send the packet out:
		 * -- to the controller
		 * -- to the switch
		 */
		List<OFAction> al = new ArrayList<OFAction>(1);

		/*
		 * If the device is known, then send a the
		 * packet-out towards it. Otherwise, flood.
		 */
		if (dstDevice != null) {
			/*
			 * We can avoid the topology service, since the only switch
			 * in this island will be this switch (sw). Furthermore,
			 * the only island a proxy's packets should ever traverse will
			 * be this switch (sw). As a result, there should only ever be 
			 * one attachment point, and it should be on our switch (sw).
			 */
			if (dstDevice.getAttachmentPoints().length == 0 || dstDevice.getAttachmentPoints().length > 1) {
				log.warn("Device should have exactly one attachment point. Got {}", dstDevice.getAttachmentPoints().toString());
			}
			for (SwitchPort sp : dstDevice.getAttachmentPoints()) {
				if (sp.getSwitchDPID().equals(sw.getId())) {
					log.debug("Determined output SwitchPort to be {}", sp.toString());
					al.add(sw.getOFFactory().actions().output(sp.getPort(), 0xFFffFFff));
					break;
				}
			}
			
			if (al.isEmpty()) {
				log.warn("Could not find a destination device attachment point. (Possible SwitchVisor bug?) Flooding packet-out on switch {}", sw.getId().toString());
				al.add(sw.getOFFactory().actions().output(OFPort.FLOOD, 0xFFffFFff));
			}
		} else {
			log.debug("Could not locate a destination device. Flooding packet-out on switch {}", sw.getId().toString());
			al.add(sw.getOFFactory().actions().output(OFPort.FLOOD, 0xFFffFFff));
		}

		/*
		 * Then, compose and write the message.
		 */
		OFPacketOut po = sw.getOFFactory().buildPacketOut()
				.setActions(al)
				.setBufferId(OFBufferId.NO_BUFFER)
				.setData(eth.serialize())
				.setInPort(OFPort.CONTROLLER)
				.setXid(sw.getOFFactory().nextXid())
				.build();
		sw.write(po);
	}

	/*
	 * IOFSwitchListener
	 */

	@Override
	public void switchAdded(DatapathId switchId) {
		/*
		 * We should only have a single switch talking to us,
		 * namely our OVS control traffic intercepter switch.
		 */
		if (intercepterSwitches.contains(switchId)) {
			log.info("Intercepter switch connected. Able to snoop on control traffic.");
		}
	}

	@Override
	public void switchRemoved(DatapathId switchId) {
		if (intercepterSwitches.contains(switchId)) {
			log.error("Intercepter switch {} removed. Did we just lose our OVS?", switchId.toString());
		}
	}

	@Override
	public void switchActivated(DatapathId switchId) { 
		if (intercepterSwitches.contains(switchId)) {
			log.warn("Intercepter switch {} activated", switchId.toString());
			log.warn("Inserting flows to forward all packets in the data plane other than TCP.");
			//TODO
		}
	}

	@Override
	public void switchPortChanged(DatapathId switchId, OFPortDesc port, PortChangeType type) {
		if (intercepterSwitches.contains(switchId)) {
			log.warn("Intercepter switch {} port {} changed " + type.toString(), switchId.toString(), port.toString());
		}
	}

	@Override
	public void switchChanged(DatapathId switchId) {
		if (intercepterSwitches.contains(switchId)) {
			log.warn("Intercepter switch {} changed", switchId.toString());
		}
	}
}