package net.floodlightcontroller.switchvisor;

import java.util.ArrayList;
import java.util.List;

import net.floodlightcontroller.util.MatchUtils;

import org.projectfloodlight.openflow.protocol.OFAggregateStatsReply;
import org.projectfloodlight.openflow.protocol.OFAggregateStatsRequest;
import org.projectfloodlight.openflow.protocol.OFBucket;
import org.projectfloodlight.openflow.protocol.OFDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFDescStatsRequest;
import org.projectfloodlight.openflow.protocol.OFErrorMsg;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFeaturesReply;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFFlowRemoved;
import org.projectfloodlight.openflow.protocol.OFFlowStatsEntry;
import org.projectfloodlight.openflow.protocol.OFFlowStatsReply;
import org.projectfloodlight.openflow.protocol.OFFlowStatsRequest;
import org.projectfloodlight.openflow.protocol.OFGroupDescStatsEntry;
import org.projectfloodlight.openflow.protocol.OFGroupDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupDescStatsRequest;
import org.projectfloodlight.openflow.protocol.OFGroupFeaturesStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupFeaturesStatsRequest;
import org.projectfloodlight.openflow.protocol.OFGroupMod;
import org.projectfloodlight.openflow.protocol.OFGroupStatsEntry;
import org.projectfloodlight.openflow.protocol.OFGroupStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupStatsRequest;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFMeterBandStats;
import org.projectfloodlight.openflow.protocol.OFMeterConfigStatsReply;
import org.projectfloodlight.openflow.protocol.OFMeterConfigStatsRequest;
import org.projectfloodlight.openflow.protocol.OFMeterFeaturesStatsReply;
import org.projectfloodlight.openflow.protocol.OFMeterFeaturesStatsRequest;
import org.projectfloodlight.openflow.protocol.OFMeterStats;
import org.projectfloodlight.openflow.protocol.OFMeterStatsReply;
import org.projectfloodlight.openflow.protocol.OFMeterStatsRequest;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFPacketQueue;
import org.projectfloodlight.openflow.protocol.OFPortDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFPortDescStatsRequest;
import org.projectfloodlight.openflow.protocol.OFPortMod;
import org.projectfloodlight.openflow.protocol.OFPortStatsEntry;
import org.projectfloodlight.openflow.protocol.OFPortStatsProp;
import org.projectfloodlight.openflow.protocol.OFPortStatsPropEthernet;
import org.projectfloodlight.openflow.protocol.OFPortStatsPropOptical;
import org.projectfloodlight.openflow.protocol.OFPortStatsReply;
import org.projectfloodlight.openflow.protocol.OFPortStatsRequest;
import org.projectfloodlight.openflow.protocol.OFPortStatus;
import org.projectfloodlight.openflow.protocol.OFQueueDesc;
import org.projectfloodlight.openflow.protocol.OFQueueDescProp;
import org.projectfloodlight.openflow.protocol.OFQueueDescPropMaxRate;
import org.projectfloodlight.openflow.protocol.OFQueueDescPropMinRate;
import org.projectfloodlight.openflow.protocol.OFQueueDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFQueueDescStatsRequest;
import org.projectfloodlight.openflow.protocol.OFQueueGetConfigReply;
import org.projectfloodlight.openflow.protocol.OFQueueGetConfigRequest;
import org.projectfloodlight.openflow.protocol.OFQueueStatsRequest;
import org.projectfloodlight.openflow.protocol.OFStatsReply;
import org.projectfloodlight.openflow.protocol.OFStatsRequest;
import org.projectfloodlight.openflow.protocol.OFTableDesc;
import org.projectfloodlight.openflow.protocol.OFTableDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFTableDescStatsRequest;
import org.projectfloodlight.openflow.protocol.OFTableFeaturesStatsReply;
import org.projectfloodlight.openflow.protocol.OFTableFeaturesStatsRequest;
import org.projectfloodlight.openflow.protocol.OFTableStatsEntry;
import org.projectfloodlight.openflow.protocol.OFTableStatsReply;
import org.projectfloodlight.openflow.protocol.OFTableStatsRequest;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionApplyActions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionWriteActions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.meterband.OFMeterBand;
import org.projectfloodlight.openflow.protocol.meterband.OFMeterBandDrop;
import org.projectfloodlight.openflow.protocol.meterband.OFMeterBandDscpRemark;
import org.projectfloodlight.openflow.protocol.meterband.OFMeterBandExperimenter;
import org.projectfloodlight.openflow.protocol.queueprop.OFQueueProp;
import org.projectfloodlight.openflow.protocol.queueprop.OFQueuePropMaxRate;
import org.projectfloodlight.openflow.protocol.queueprop.OFQueuePropMinRate;
import org.projectfloodlight.openflow.protocol.ver13.OFQueuePropertiesSerializerVer13;
import org.projectfloodlight.openflow.protocol.ver14.OFMeterBandTypeSerializerVer14;
import org.projectfloodlight.openflow.protocol.ver14.OFPortStatsPropTypeSerializerVer14;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OFPacketMangler {
	private static final Logger log = LoggerFactory.getLogger(OFPacketMangler.class);

	public static OFMessage mangle(OFMessage m, ProxyInfo pi, PacketDirection pd) {
		/*
		 * The new message that we'll save.
		 */
		OFMessage mangled = null;

		/*
		 * What OF message types do we want
		 * to ignore? Not much.
		 */
		switch (m.getType()) {
		case BARRIER_REPLY:
		case BARRIER_REQUEST:
		case BUNDLE_ADD_MESSAGE:
		case BUNDLE_CONTROL:
		case ECHO_REPLY:
		case ECHO_REQUEST:
		case EXPERIMENTER:
		case FEATURES_REQUEST: /* We will note the OpenFlow version chosen by the handshake in the reply only. */
		case GET_ASYNC_REPLY:
		case GET_ASYNC_REQUEST:
		case GET_CONFIG_REPLY:
		case GET_CONFIG_REQUEST:
		case HELLO:
		case METER_MOD:
		case REQUESTFORWARD:
		case ROLE_REPLY:
		case ROLE_REQUEST:
		case ROLE_STATUS:
		case SET_ASYNC:
		case SET_CONFIG:
		case TABLE_MOD:
		case TABLE_STATUS:
			log.debug("Not mangling message type {}.", m.getType().toString());
			mangled = m;
			break;
		case ERROR:
			mangled = mangleError((OFErrorMsg) m, pi, pd);
			break;
		case FEATURES_REPLY:
			mangled = mangleFeaturesReply((OFFeaturesReply) m, pi, pd);
			break;		
		case FLOW_MOD:
			mangled = mangleFlowMod((OFFlowMod) m, pi, pd);
			break;
		case FLOW_REMOVED:
			mangled = mangleFlowRemoved((OFFlowRemoved) m, pi, pd);
			break;
		case GROUP_MOD:
			mangled = mangleGroupMod((OFGroupMod) m, pi, pd);
			break;
		case PACKET_IN:
			mangled = manglePacketIn((OFPacketIn) m, pi, pd);
			break;
		case PACKET_OUT:
			mangled = manglePacketOut((OFPacketOut) m, pi, pd);
			break;
		case PORT_MOD:
			mangled = manglePortMod((OFPortMod) m, pi, pd);
			break;
		case PORT_STATUS:
			mangled = manglePortStatus((OFPortStatus) m, pi, pd);
			break;
		case QUEUE_GET_CONFIG_REPLY:
			mangled = mangleQueueGetConfigReply((OFQueueGetConfigReply) m, pi, pd);
			break;
		case QUEUE_GET_CONFIG_REQUEST:
			mangled = mangleQueueGetConfigRequest((OFQueueGetConfigRequest) m, pi, pd);
			break;
		case STATS_REPLY:
			mangled = mangleStatsReply((OFStatsReply) m, pi, pd);
			break;
		case STATS_REQUEST: /* A.K.A. "multipart" */
			mangled = mangleStatsRequest((OFStatsRequest<?>) m, pi, pd);
			break;
		default:
			log.warn("Got unhandled message type of {}. Message will not be mangled.", m.getType().toString());
			mangled = m;
			break;
		}
		return mangled;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * For all intents and purposes, this is the exact same as 
	 * {@link OFPacketMangler#mangleFlowStatsRequest(OFAggregateStatsRequest, ProxyInfo, PacketDirection)}
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFAggregateStatsRequest mangleAggregateStatsRequest(OFAggregateStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFAggregateStatsRequest.Builder b = f.buildAggregateStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setOutPort(pi.getPhysicalPortFromProxy(m.getOutPort()))
		.setMatch(mangleMatch(m.getMatch(), pi, pd))
		.setTableId(m.getTableId())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
			m = b.build();
			break;
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.setOutGroup(m.getOutGroup())
			.setCookie(m.getCookie())
			.setCookieMask(m.getCookieMask())
			.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFDescStatsRequest mangleDescriptionStatsRequest(OFDescStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFDescStatsRequest.Builder b = f.buildDescStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * For all intents and purposes, this is the exact same as 
	 * {@link OFPacketMangler#mangleAggregateStatsRequest(OFAggregateStatsRequest, ProxyInfo, PacketDirection)}
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFFlowStatsRequest mangleFlowStatsRequest(OFFlowStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFFlowStatsRequest.Builder b = f.buildFlowStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setOutPort(pi.getPhysicalPortFromProxy(m.getOutPort()))
		.setMatch(mangleMatch(m.getMatch(), pi, pd))
		.setTableId(m.getTableId())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
			m = b.build();
			break;
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.setOutGroup(m.getOutGroup())
			.setCookie(m.getCookie())
			.setCookieMask(m.getCookieMask())
			.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFGroupStatsRequest mangleGroupStatsRequest(OFGroupStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFGroupStatsRequest.Builder b = f.buildGroupStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setGroup(m.getGroup())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFGroupDescStatsRequest mangleGroupDescriptionStatsRequest(OFGroupDescStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFGroupDescStatsRequest.Builder b = f.buildGroupDescStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFGroupFeaturesStatsRequest mangleGroupFeaturesStatsRequest(OFGroupFeaturesStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFGroupFeaturesStatsRequest.Builder b = f.buildGroupFeaturesStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFMeterStatsRequest mangleMeterStatsRequest(OFMeterStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFMeterStatsRequest.Builder b = f.buildMeterStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setMeterId(m.getMeterId())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFMeterConfigStatsRequest mangleMeterConfigStatsRequest(OFMeterConfigStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFMeterConfigStatsRequest.Builder b = f.buildMeterConfigStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setMeterId(m.getMeterId())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFMeterFeaturesStatsRequest mangleMeterFeaturesStatsRequest(OFMeterFeaturesStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFMeterFeaturesStatsRequest.Builder b = f.buildMeterFeaturesStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPortStatsRequest manglePortStatsRequest(OFPortStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFPortStatsRequest.Builder b = f.buildPortStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setPortNo(pi.getPhysicalPortFromProxy(m.getPortNo()))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPortDescStatsRequest manglePortDescriptionStatsRequest(OFPortDescStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFPortDescStatsRequest.Builder b = f.buildPortDescStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;		
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFQueueStatsRequest mangleQueueStatsRequest(OFQueueStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFQueueStatsRequest.Builder b = f.buildQueueStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setPortNo(pi.getPhysicalPortFromProxy(m.getPortNo()))
		.setQueueId(m.getQueueId())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;		
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFQueueDescStatsRequest mangleQueueDescriptionStatsRequest(OFQueueDescStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFQueueDescStatsRequest.Builder b = f.buildQueueDescStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;	
		case OF_10:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFTableStatsRequest mangleTableStatsRequest(OFTableStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFTableStatsRequest.Builder b = f.buildTableStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;	
		case OF_10:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFTableDescStatsRequest mangleTableDescriptionStatsRequest(OFTableDescStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFTableDescStatsRequest.Builder b = f.buildTableDescStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_14:
			m = b.build();
			break;	
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsRequest(OFStatsRequest, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (controller/proxy to switch only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFTableFeaturesStatsRequest mangleTableFeaturesStatsRequest(OFTableFeaturesStatsRequest m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
		OFTableFeaturesStatsRequest.Builder b = f.buildTableFeaturesStatsRequest();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(m.getEntries())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;	
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * First function to process any stats/multipart request messages. Helper functions are used for specific
	 * message type processing. This parent function guarantees to the helper functions that the packet
	 * direction is correct prior to invoking them.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFStatsRequest<?> mangleStatsRequest(OFStatsRequest<?> m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PHYS_TO_PROXY:
			log.error("Cannot send stats/multipart request {} from switch to controller.", ((OFStatsRequest<?>) m).getStatsType().toString());
			break;
		case PROXY_TO_PHYS:
			switch (((OFStatsRequest<?>) m).getStatsType()) {
			case AGGREGATE:
				m = mangleAggregateStatsRequest((OFAggregateStatsRequest) m, pi, pd);
				break;
			case DESC:
				m = mangleDescriptionStatsRequest((OFDescStatsRequest) m, pi, pd);
				break;
			case FLOW:
				m = mangleFlowStatsRequest((OFFlowStatsRequest) m, pi, pd);
				break;
			case GROUP:
				m = mangleGroupStatsRequest((OFGroupStatsRequest) m, pi, pd);
				break;
			case GROUP_DESC:
				m = mangleGroupDescriptionStatsRequest((OFGroupDescStatsRequest) m, pi, pd);
				break;
			case GROUP_FEATURES:
				m = mangleGroupFeaturesStatsRequest((OFGroupFeaturesStatsRequest) m, pi, pd);
				break;
			case METER:
				m = mangleMeterStatsRequest((OFMeterStatsRequest) m, pi, pd);
				break;
			case METER_CONFIG:
				m = mangleMeterConfigStatsRequest((OFMeterConfigStatsRequest) m, pi, pd);
				break;
			case METER_FEATURES:
				m = mangleMeterFeaturesStatsRequest((OFMeterFeaturesStatsRequest) m, pi, pd);
				break;
			case PORT:
				m = manglePortStatsRequest((OFPortStatsRequest) m, pi, pd);
				break;
			case PORT_DESC:
				m = manglePortDescriptionStatsRequest((OFPortDescStatsRequest) m, pi, pd);
				break;
			case QUEUE:
				m = mangleQueueStatsRequest((OFQueueStatsRequest) m, pi, pd);
				break;
			case QUEUE_DESC:
				m = mangleQueueDescriptionStatsRequest((OFQueueDescStatsRequest) m, pi, pd);
				break;
			case TABLE:
				m = mangleTableStatsRequest((OFTableStatsRequest) m, pi, pd);
				break;
			case TABLE_DESC:
				m = mangleTableDescriptionStatsRequest((OFTableDescStatsRequest) m, pi, pd);
				break;
			case TABLE_FEATURES:
				m = mangleTableFeaturesStatsRequest((OFTableFeaturesStatsRequest) m, pi, pd);
				break;
			case EXPERIMENTER:
			case FLOW_MONITOR:
			default:
				log.error("Unhandled stats/multipart request type {}. Message will not be mangled.", ((OFStatsRequest<?>) m).getStatsType().toString());
				break;
			}
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFAggregateStatsReply mangleAggregateStatsReply(OFAggregateStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFAggregateStatsReply.Builder b = f.buildAggregateStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setByteCount(m.getByteCount())
		.setFlowCount(m.getFlowCount())
		.setPacketCount(m.getPacketCount())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFDescStatsReply mangleDescriptionStatsReply(OFDescStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFDescStatsReply.Builder b = f.buildDescStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setDpDesc(m.getDpDesc())
		.setHwDesc(m.getHwDesc())
		.setMfrDesc(m.getMfrDesc())
		.setSerialNum(m.getSerialNum())
		.setSwDesc(m.getSwDesc())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFFlowStatsReply mangleFlowStatsReply(OFFlowStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFFlowStatsReply.Builder b = f.buildFlowStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleFlowStatsEntry(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleFlowStatsReply(OFFlowStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFFlowStatsEntry> mangleFlowStatsEntry(List<OFFlowStatsEntry> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFFlowStatsEntry> nel = new ArrayList<OFFlowStatsEntry>(el.size());
		for (OFFlowStatsEntry e : el) {
			OFFlowStatsEntry.Builder b = f.buildFlowStatsEntry();
			b.setByteCount(e.getByteCount())
			.setCookie(e.getCookie())
			.setDurationNsec(e.getDurationNsec())
			.setDurationSec(e.getDurationSec())
			.setFlags(e.getFlags())
			.setHardTimeout(e.getHardTimeout())
			.setIdleTimeout(e.getIdleTimeout())
			.setMatch(mangleMatch(e.getMatch(), pi, pd))
			.setPacketCount(e.getPacketCount())
			.setPriority(e.getPriority())
			.setTableId(e.getTableId());
			switch (f.getVersion()) {
			case OF_10:
				nel.add(b.setActions(mangleActions(e.getActions(), pi, pd))
						.build());
				break;
			case OF_11:
			case OF_12:	
			case OF_13:
				nel.add(b.setInstructions(mangleInstructions(e.getInstructions(), pi, pd))
						.build());
				break;
			case OF_14:
				nel.add(b.setInstructions(mangleInstructions(e.getInstructions(), pi, pd))
						.setImportance(e.getImportance())
						.build());
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFFlowStatsEntry {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFGroupStatsReply mangleGroupStatsReply(OFGroupStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFGroupStatsReply.Builder b = f.buildGroupStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleGroupStatsEntry(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleGroupStatsReply(OFGroupStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFGroupStatsEntry> mangleGroupStatsEntry(List<OFGroupStatsEntry> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFGroupStatsEntry> nel = new ArrayList<OFGroupStatsEntry>(el.size());
		for (OFGroupStatsEntry e : el) {
			OFGroupStatsEntry.Builder b = f.buildGroupStatsEntry();
			b.setByteCount(e.getByteCount())
			.setDurationNsec(e.getDurationNsec())
			.setDurationSec(e.getDurationSec())
			.setPacketCount(e.getPacketCount())
			.setRefCount(e.getRefCount())
			.setBucketStats(e.getBucketStats())
			.setGroup(e.getGroup());
			switch (f.getVersion()) {
			case OF_11:
			case OF_12:	
			case OF_13:
			case OF_14:
				nel.add(b.build());
				break;
			case OF_10:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFGroupStatsEntry {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFGroupDescStatsReply mangleGroupDescriptionStatsReply(OFGroupDescStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFGroupDescStatsReply.Builder b = f.buildGroupDescStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleGroupDescriptionStatsEntry(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleGroupDescStatsReply(OFGroupDescStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFGroupDescStatsEntry> mangleGroupDescriptionStatsEntry(List<OFGroupDescStatsEntry> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFGroupDescStatsEntry> nel = new ArrayList<OFGroupDescStatsEntry>(el.size());
		for (OFGroupDescStatsEntry e : el) {
			OFGroupDescStatsEntry.Builder b = f.buildGroupDescStatsEntry();
			b.setGroup(e.getGroup())
			.setGroupType(e.getGroupType())
			.setBuckets(mangleBuckets(e.getBuckets(), pi, pd));
			switch (f.getVersion()) {
			case OF_11:
			case OF_12:	
			case OF_13:
			case OF_14:
				nel.add(b.build());
				break;
			case OF_10:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFGroupDescStatsEntry {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFGroupFeaturesStatsReply mangleGroupFeaturesStatsReply(OFGroupFeaturesStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFGroupFeaturesStatsReply.Builder b = f.buildGroupFeaturesStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setActionsAll(m.getActionsAll())
		.setActionsFf(m.getActionsFf())
		.setActionsIndirect(m.getActionsIndirect())
		.setActionsSelect(m.getActionsSelect())
		.setCapabilities(m.getCapabilities())
		.setMaxGroupsAll(m.getMaxGroupsAll())
		.setMaxGroupsFf(m.getMaxGroupsFf())
		.setMaxGroupsIndirect(m.getMaxGroupsIndirect())
		.setMaxGroupsSelect(m.getMaxGroupsSelect())
		.setTypes(m.getTypes())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFMeterStatsReply mangleMeterStatsReply(OFMeterStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFMeterStatsReply.Builder b = f.buildMeterStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleMeterStats(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleMeterStatsReply(OFMeterStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFMeterStats> mangleMeterStats(List<OFMeterStats> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFMeterStats> nel = new ArrayList<OFMeterStats>(el.size());
		for (OFMeterStats e : el) {
			OFMeterStats.Builder b = f.buildMeterStats();
			b.setByteInCount(e.getByteInCount())
			.setBandStats(mangleMeterBandStats(e.getBandStats(), pi, pd))
			.setDurationNsec(e.getDurationNsec())
			.setDurationSec(e.getDurationSec())
			.setFlowCount(e.getFlowCount())
			.setMeterId(e.getMeterId())
			.setPacketInCount(e.getPacketInCount());
			switch (f.getVersion()) {
			case OF_13:
			case OF_14:
				nel.add(b.build());
				break;
			case OF_10:
			case OF_11:
			case OF_12:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMeterStats {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleMeterStats(List<OFMeterStats>, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFMeterBandStats> mangleMeterBandStats(List<OFMeterBandStats> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFMeterBandStats> nel = new ArrayList<OFMeterBandStats>(el.size());
		for (OFMeterBandStats e : el) {
			OFMeterBandStats.Builder b = f.buildMeterBandStats();
			b.setByteBandCount(e.getByteBandCount())
			.setPacketBandCount(e.getPacketBandCount());
			switch (f.getVersion()) {
			case OF_13:
			case OF_14:
				nel.add(b.build());
				break;
			case OF_10:
			case OF_11:
			case OF_12:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMeterBandStats {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFMeterConfigStatsReply mangleMeterConfigStatsReply(OFMeterConfigStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFMeterConfigStatsReply.Builder b = f.buildMeterConfigStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleMeterBands(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleMeterConfigStatsReply(List<OFMeterBand>, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFMeterBand> mangleMeterBands(List<OFMeterBand> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFMeterBand> nel = new ArrayList<OFMeterBand>(el.size());
		for (OFMeterBand e : el) {
			OFMeterBand.Builder b;
			switch (e.getType()) {
			case OFMeterBandTypeSerializerVer14.DROP_VAL:
				b = f.meterBands().buildDrop()
				.setBurstSize(((OFMeterBandDrop) e).getBurstSize())
				.setRate(((OFMeterBandDrop) e).getRate());
				break;
			case OFMeterBandTypeSerializerVer14.DSCP_REMARK_VAL:
				b = f.meterBands().buildDscpRemark()
				.setBurstSize(((OFMeterBandDscpRemark) e).getBurstSize())
				.setRate(((OFMeterBandDscpRemark) e).getRate())
				.setPrecLevel(((OFMeterBandDscpRemark) e).getPrecLevel());
				break;
			case OFMeterBandTypeSerializerVer14.EXPERIMENTER_VAL:
			default:
				b = f.meterBands().buildExperimenter()
				.setBurstSize(((OFMeterBandExperimenter) e).getBurstSize())
				.setRate(((OFMeterBandExperimenter) e).getRate())
				.setExperimenter(((OFMeterBandExperimenter) e).getExperimenter());
				break;
			}
			switch (f.getVersion()) {
			case OF_13:
			case OF_14:
				nel.add(b.build());
				break;
			case OF_10:
			case OF_11:
			case OF_12:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMeterBandStats {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFMeterFeaturesStatsReply mangleMeterFeaturesStatsReply(OFMeterFeaturesStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFMeterFeaturesStatsReply.Builder b = f.buildMeterFeaturesStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setFeatures(m.getFeatures())
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPortStatsReply manglePortStatsReply(OFPortStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFPortStatsReply.Builder b = f.buildPortStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(manglePortStatsEntries(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#manglePortStats(OFPortStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFPortStatsEntry> manglePortStatsEntries(List<OFPortStatsEntry> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFPortStatsEntry> nel = new ArrayList<OFPortStatsEntry>(el.size());
		for (OFPortStatsEntry e : el) {
			OFPortStatsEntry.Builder b = f.buildPortStatsEntry();
			b.setCollisions(e.getCollisions())
			.setDurationNsec(e.getDurationNsec())
			.setDurationSec(e.getDurationSec())
			.setPortNo(pi.getProxyPortFromPhysical(e.getPortNo())) /* handle the port -- only direction is from switch to controller */
			.setRxBytes(e.getRxBytes())
			.setRxCrcErr(e.getRxCrcErr())
			.setRxDropped(e.getRxDropped())
			.setRxErrors(e.getRxErrors())
			.setRxFrameErr(e.getRxFrameErr())
			.setRxOverErr(e.getRxOverErr())
			.setRxPackets(e.getRxPackets())
			.setTxBytes(e.getTxBytes())
			.setTxDropped(e.getTxDropped())
			.setTxErrors(e.getTxErrors())
			.setTxPackets(e.getTxPackets());
			switch (f.getVersion()) {
			case OF_14:
				b.setProperties(manglePortStatsProperties(e.getProperties(), pi, pd)); /* include for 1.4, and fall through */
			case OF_10:
			case OF_11:
			case OF_12:
			case OF_13:
				nel.add(b.build());
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFPortStatsEntry {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#manglePortStatsEntries(List<OFPortStatsEntry>, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFPortStatsProp> manglePortStatsProperties(List<OFPortStatsProp> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFPortStatsProp> nel = new ArrayList<OFPortStatsProp>(el.size());
		for (OFPortStatsProp e : el) {
			OFPortStatsProp.Builder b = null;
			switch (e.getType()) {
			case OFPortStatsPropTypeSerializerVer14.ETHERNET_VAL: /* only defined in OF1.4+ */
				b = f.buildPortStatsPropEthernet()
				.setCollisions(((OFPortStatsPropEthernet) e).getCollisions())
				.setRxCrcErr(((OFPortStatsPropEthernet) e).getRxCrcErr())
				.setRxFrameErr(((OFPortStatsPropEthernet) e).getRxFrameErr())
				.setRxOverErr(((OFPortStatsPropEthernet) e).getRxOverErr());
				break;
			case OFPortStatsPropTypeSerializerVer14.OPTICAL_VAL:
				b = f.buildPortStatsPropOptical()
				.setBiasCurrent(((OFPortStatsPropOptical) e).getBiasCurrent())
				.setFlags(((OFPortStatsPropOptical) e).getFlags())
				.setRxFreqLmda(((OFPortStatsPropOptical) e).getRxFreqLmda())
				.setRxGridSpan(((OFPortStatsPropOptical) e).getRxGridSpan())
				.setRxOffset(((OFPortStatsPropOptical) e).getRxOffset())
				.setRxPwr(((OFPortStatsPropOptical) e).getRxPwr())
				.setTemperature(((OFPortStatsPropOptical) e).getTemperature())
				.setTxFreqLmda(((OFPortStatsPropOptical) e).getTxFreqLmda())
				.setTxGridSpan(((OFPortStatsPropOptical) e).getTxGridSpan())
				.setTxOffset(((OFPortStatsPropOptical) e).getTxOffset())
				.setTxPwr(((OFPortStatsPropOptical) e).getTxPwr());
				break;
			case OFPortStatsPropTypeSerializerVer14.EXPERIMENTER_VAL:
			default:
				log.error("Unsupported OFPortStatsPropType {}. Not including it in the list.", e.getType());
				break;
			}
			if (b != null) {
				switch (f.getVersion()) {
				case OF_14:
					nel.add(b.build());
					break;
				case OF_10:
				case OF_11:
				case OF_12:
				case OF_13:
				default:
					log.error("Unsupported OpenFlow version received from {} for OFPortStatsEntry {}", pd, e);
					break;
				}
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPortDescStatsReply manglePortDescriptionStatsReply(OFPortDescStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFPortDescStatsReply.Builder b = f.buildPortDescStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(pi.getProxyPortDescFromPhysical(m.getEntries()))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFQueueDescStatsReply mangleQueueDescriptionStatsReply(OFQueueDescStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFQueueDescStatsReply.Builder b = f.buildQueueDescStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleQueueDescriptionEntries(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleQueueDescStats(OFQueueDescStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFQueueDesc> mangleQueueDescriptionEntries(List<OFQueueDesc> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFQueueDesc> nel = new ArrayList<OFQueueDesc>(el.size());
		for (OFQueueDesc e : el) {
			OFQueueDesc.Builder b = f.buildQueueDesc();
			b.setQueueId(e.getQueueId())
			.setPortNo(pi.getProxyPortFromPhysical(OFPort.of((int)e.getPortNo())).getPortNumber()); /* handle the port -- only direction is from switch to controller */
			switch (f.getVersion()) {
			case OF_14:
				b.setProperties(mangleQueueDescriptionProperties(e.getProperties(), pi, pd));  /* include for 1.4, and fall through */
			case OF_10:
			case OF_11:
			case OF_12:
			case OF_13:
				nel.add(b.build());
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFPortStatsEntry {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleQueueDescriptionEntries(List<OFQueueDesc>, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFQueueDescProp> mangleQueueDescriptionProperties(List<OFQueueDescProp> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFQueueDescProp> nel = new ArrayList<OFQueueDescProp>(el.size());
		for (OFQueueDescProp e : el) {
			OFQueueDescProp.Builder b = null;
			switch (e.getType()) {
			case OFQueuePropertiesSerializerVer13.MAX_RATE_VAL: /* only defined in OF1.2+ */
				b = f.buildQueueDescPropMaxRate()
				.setRate(((OFQueueDescPropMaxRate) e).getRate());
				break;
			case OFQueuePropertiesSerializerVer13.MIN_RATE_VAL:
				b = f.buildQueueDescPropMinRate()
				.setRate(((OFQueueDescPropMinRate) e).getRate());
				break;
			case OFPortStatsPropTypeSerializerVer14.EXPERIMENTER_VAL:
			default:
				log.error("Unsupported OFPortQueueDescPropType {}. Not including it in the list.", e.getType());
				break;
			}
			if (b != null) {
				switch (f.getVersion()) {
				case OF_10:
				case OF_11:
				case OF_12:
				case OF_13:
				case OF_14:
					nel.add(b.build());
					break;
				default:
					log.error("Unsupported OpenFlow version received from {} for OFQueueDescProp {}", pd, e);
					break;
				}
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFTableStatsReply mangleTableStatsReply(OFTableStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFTableStatsReply.Builder b = f.buildTableStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleTableStatsEntries(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleTableStatsReply(OFTableStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFTableStatsEntry> mangleTableStatsEntries(List<OFTableStatsEntry> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFTableStatsEntry> nel = new ArrayList<OFTableStatsEntry>(el.size());
		for (OFTableStatsEntry e : el) {
			OFTableStatsEntry.Builder b = f.buildTableStatsEntry();
			b.setActiveCount(e.getActiveCount())
			.setApplyActions(e.getApplyActions())
			.setApplySetfields(e.getApplySetfields())
			.setConfig(e.getConfig())
			.setInstructions(e.getInstructions())
			.setLookupCount(e.getLookupCount())
			.setMatch(e.getMatch())
			.setMatchedCount(e.getMatchedCount())
			.setMaxEntries(e.getMaxEntries())
			.setMetadataMatch(e.getMetadataMatch())
			.setMetadataWrite(e.getMetadataWrite())
			.setName(e.getName())
			.setTableId(e.getTableId())
			.setWildcards(e.getWildcards())
			.setWriteActions(e.getWriteActions())
			.setWriteSetfields(e.getWriteSetfields());

			switch (f.getVersion()) {
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				nel.add(b.build());
				break;
			case OF_10:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFTableStatsEntry {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFTableDescStatsReply mangleTableDescriptionStatsReply(OFTableDescStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFTableDescStatsReply.Builder b = f.buildTableDescStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(mangleTableDescriptionEntries(m.getEntries(), pi, pd))
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_10:
		case OF_11:
		case OF_12:	
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleTableDescriptionStatsReply(OFTableDescStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFTableDesc> mangleTableDescriptionEntries(List<OFTableDesc> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		List<OFTableDesc> nel = new ArrayList<OFTableDesc>(el.size());
		for (OFTableDesc e : el) {
			OFTableDesc.Builder b = f.buildTableDesc();
			b.setConfig(e.getConfig())
			.setTableId(e.getTableId());

			switch (f.getVersion()) {
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				nel.add(b.build());
				break;
			case OF_10:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFTableDesc {}", pd, e);
				break;
			}
		}
		return nel;
	}

	/**
	 * Helper function of {@link OFPacketMangler#mangleStatsReply(OFStatsReply, ProxyInfo, PacketDirection)}.
	 * Assumes PacketDirection is correct (switch to controller/proxy only).
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFTableFeaturesStatsReply mangleTableFeaturesStatsReply(OFTableFeaturesStatsReply m, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
		OFTableFeaturesStatsReply.Builder b = f.buildTableFeaturesStatsReply();
		/* Take care of the stuff in common to all versions first */
		b.setFlags(m.getFlags())
		.setEntries(m.getEntries()) /* The entries only express capabilities of a table and have nothing to do with ports. */
		.setXid(m.getXid());
		switch (f.getVersion()) {
		case OF_13:
		case OF_14:
			m = b.build();
			break;
		case OF_10:
		case OF_11:
		case OF_12:	
		default:
			log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			break;
		}
		return m;
	}

	/**
	 * First function to process any stats/multipart reply messages. Helper functions are used for specific
	 * message type processing. This parent function guarantees to the helper functions that the packet
	 * direction is correct prior to invoking them.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFStatsReply mangleStatsReply(OFStatsReply m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			log.error("Cannot send stats/multipart reply {} from controller to switch.", ((OFStatsReply) m).getStatsType().toString());
			break;
		case PHYS_TO_PROXY:
			switch (((OFStatsReply) m).getStatsType()) {
			case AGGREGATE:
				m = mangleAggregateStatsReply((OFAggregateStatsReply) m, pi, pd);
				break;
			case DESC:
				m = mangleDescriptionStatsReply((OFDescStatsReply) m, pi, pd);
				break;
			case FLOW:
				m = mangleFlowStatsReply((OFFlowStatsReply) m, pi, pd);
				break;
			case GROUP:
				m = mangleGroupStatsReply((OFGroupStatsReply) m, pi, pd);
				break;
			case GROUP_DESC:
				m = mangleGroupDescriptionStatsReply((OFGroupDescStatsReply) m, pi, pd);
				break;
			case GROUP_FEATURES:
				m = mangleGroupFeaturesStatsReply((OFGroupFeaturesStatsReply) m, pi, pd);
				break;
			case METER:
				m = mangleMeterStatsReply((OFMeterStatsReply) m, pi, pd);
				break;
			case METER_CONFIG:
				m = mangleMeterConfigStatsReply((OFMeterConfigStatsReply) m, pi, pd);
				break;
			case METER_FEATURES:
				m = mangleMeterFeaturesStatsReply((OFMeterFeaturesStatsReply) m, pi, pd);
				break;
			case PORT:
				m = manglePortStatsReply((OFPortStatsReply) m, pi, pd);
				break;
			case PORT_DESC:
				m = manglePortDescriptionStatsReply((OFPortDescStatsReply) m, pi, pd);
				break;
			case QUEUE: /* TODO does Loxi not implement this? */
				log.error("Unhandled stats/multipart request type {}. Message will not be mangled.", ((OFStatsReply) m).getStatsType().toString());
				break;
			case QUEUE_DESC:
				m = mangleQueueDescriptionStatsReply((OFQueueDescStatsReply) m, pi, pd);
				break;
			case TABLE:
				m = mangleTableStatsReply((OFTableStatsReply) m, pi, pd);
				break;
			case TABLE_DESC:
				m = mangleTableDescriptionStatsReply((OFTableDescStatsReply) m, pi, pd);
				break;
			case TABLE_FEATURES:
				m = mangleTableFeaturesStatsReply((OFTableFeaturesStatsReply) m, pi, pd);
				break;
			case EXPERIMENTER:
			case FLOW_MONITOR:
			default:
				log.error("Unhandled stats/multipart request type {}. Message will not be mangled.", ((OFStatsReply) m).getStatsType().toString());
				break;
			}
		}
		return m;
	}

	/**
	 * Only OF1.4 is different w/properties. The port number is the only thing mangled.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPortMod manglePortMod(OFPortMod m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PHYS_TO_PROXY:
			log.error("Cannot send port mod from switch to controller.");
			break;
		case PROXY_TO_PHYS:
			OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			OFPortMod.Builder b = f.buildPortMod();
			switch (f.getVersion()) {
			case OF_14:
				b.setProperties(m.getProperties()); /* added in OF1.4 */
			case OF_10:
			case OF_11:
			case OF_12:
			case OF_13:
				m = b.setAdvertise(m.getAdvertise())
				.setConfig(m.getConfig())
				.setHwAddr(m.getHwAddr())
				.setMask(m.getMask())
				.setPortNo(pi.getPhysicalPortFromProxy(m.getPortNo()))
				.setXid(m.getXid())
				.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
				break;
			}
		}
		return m;
	}

	/**
	 * The port number the queue is associated with is the only thing mangled.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFQueueGetConfigRequest mangleQueueGetConfigRequest(OFQueueGetConfigRequest m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PHYS_TO_PROXY:
			log.error("Cannot send queue get config request from switch to controller.");
			break;
		case PROXY_TO_PHYS:
			OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			OFQueueGetConfigRequest.Builder b = f.buildQueueGetConfigRequest();
			b.setPort(pi.getPhysicalPortFromProxy(m.getPort()))
			.setXid(m.getXid());
			switch (f.getVersion()) {
			case OF_10:
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
				break;
			}
		}
		return m;
	}

	/**
	 * The port number the queue is associated with and the queues are the only things mangled.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFQueueGetConfigReply mangleQueueGetConfigReply(OFQueueGetConfigReply m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			log.error("Cannot send queue get config reply from switch to controller.");
			break;
		case PHYS_TO_PROXY:
			OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
			OFQueueGetConfigReply.Builder b = f.buildQueueGetConfigReply();
			b.setPort(pi.getProxyPortFromPhysical(m.getPort()))
			.setQueues(manglePacketQueues(m.getQueues(), pi, pd))
			.setXid(m.getXid());
			switch (f.getVersion()) {
			case OF_10:
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
				break;
			}
		}
		return m;
	}

	/**
	 * Helper function for @link{OFPacketMangler#mangleQueueGetConfigReply(OFQueueGetConfigReply, ProxyInfo, PacketDirection)}.
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFPacketQueue> manglePacketQueues(List<OFPacketQueue> ql, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = null;
		switch (pd) {
		case PROXY_TO_PHYS:
			f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			break;
		case PHYS_TO_PROXY:
			f = OFFactories.getFactory(pi.getProxySwitchVersion());
			break;
		default:
			log.error("Could not determine packet direction {} while mangling packet queues", pd);
			return ql;
		}

		List<OFPacketQueue> nql = new ArrayList<OFPacketQueue>(ql.size());
		for (OFPacketQueue q : ql) {
			OFPacketQueue.Builder b = f.buildPacketQueue();
			b.setProperties(mangleQueueProperties(q.getProperties(), pi, pd))
			.setQueueId(q.getQueueId());
			switch (pd) {
			case PROXY_TO_PHYS:
				b.setPort(pi.getPhysicalPortFromProxy(q.getPort()));
				break;
			case PHYS_TO_PROXY:
				b.setPort(pi.getProxyPortFromPhysical(q.getPort()));
				break;
			}
			switch (f.getVersion()) {
			case OF_10:
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				nql.add(b.build());
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, q);
				break;
			}
		}

		return nql;
	}

	/**
	 * Helper function of {@link OFPacketMangler#manglePacketQueues(List<OFPacketQueue>, ProxyInfo, PacketDirection)}.
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFQueueProp> mangleQueueProperties(List<OFQueueProp> el, ProxyInfo pi, PacketDirection pd) {
		OFFactory f = null;
		switch (pd) {
		case PROXY_TO_PHYS:
			f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			break;
		case PHYS_TO_PROXY:
			f = OFFactories.getFactory(pi.getProxySwitchVersion());
			break;
		default:
			log.error("Could not determine packet direction {} while mangling packet queue properties", pd);
			return el;
		}

		List<OFQueueProp> nel = new ArrayList<OFQueueProp>(el.size());
		for (OFQueueProp e : el) {
			OFQueueProp.Builder b = null;
			switch (e.getType()) {
			case OFQueuePropertiesSerializerVer13.MAX_RATE_VAL: /* only defined in OF1.2+ */
				b = f.queueProps().buildMaxRate()
				.setRate(((OFQueuePropMaxRate) e).getRate());
				break;
			case OFQueuePropertiesSerializerVer13.MIN_RATE_VAL:
				b = f.queueProps().buildMinRate()
				.setRate(((OFQueuePropMinRate) e).getRate());
				break;
			case OFPortStatsPropTypeSerializerVer14.EXPERIMENTER_VAL:
			default:
				log.error("Unsupported OFPortQueuePropsType {}. Not including it in the list.", e.getType());
				break;
			}
			if (b != null) {
				switch (f.getVersion()) {
				case OF_10:
				case OF_11:
				case OF_12:
				case OF_13:
				case OF_14:
					nel.add(b.build());
					break;
				default:
					log.error("Unsupported OpenFlow version received from {} for OFQueueProp {}", pd, e);
					break;
				}
			}
		}
		return nel;
	}


	/**
	 * Pretty straightforward -- just convert the port description. Common format for OF1.0-1.4 (yay!).
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPortStatus manglePortStatus(OFPortStatus m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			log.error("Cannot send port status from controller to switch.");
			break;
		case PHYS_TO_PROXY:
			OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
			OFPortStatus.Builder b = f.buildPortStatus();
			switch (f.getVersion()) {
			case OF_10:
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.setReason(m.getReason())
				.setDesc(pi.getPhysicalPortDescFromProxy(m.getDesc()))
				.setXid(m.getXid())
				.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
				break;
			}
		}
		return m;
	}

	/**
	 * Only the match needs to be changed -- might contain ports.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFFlowRemoved mangleFlowRemoved(OFFlowRemoved m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			log.error("Cannot send flow removed from controller to switch.");
			break;
		case PHYS_TO_PROXY:
			OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
			OFFlowRemoved.Builder b = f.buildFlowRemoved();

			b.setMatch(mangleMatch(m.getMatch(), pi, pd))
			.setCookie(m.getCookie())
			.setIdleTimeout(m.getIdleTimeout())
			.setPriority(m.getPriority())
			.setReason(m.getReason())
			.setDurationSec(m.getDurationSec())
			.setDurationNsec(m.getDurationNsec())
			.setPacketCount(m.getPacketCount())
			.setByteCount(m.getByteCount())
			.setXid(m.getXid());

			switch (f.getVersion()) {
			case OF_10:
				m = b.build();
				break;
			case OF_11:
				m = b.setTableId(m.getTableId())
				.build();
				break;
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.setTableId(m.getTableId())
				.setHardTimeout(m.getHardTimeout())
				.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
				break;
			}

			break;
		}
		return m;
	}

	/**
	 * Flow mods require we handled the command and the instructions/actions.
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFFlowMod mangleFlowMod(OFFlowMod m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PHYS_TO_PROXY:
			log.error("Cannot send flow mod from switch to controller.");
			break;
		case PROXY_TO_PHYS:
			OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			OFFlowMod.Builder b;
			log.info("Flow mod before: {}", m);
			switch (m.getCommand()) {
			case ADD:
				b = f.buildFlowAdd();
				break;
			case DELETE:
				b = f.buildFlowDelete();
				break;
			case DELETE_STRICT:
				b = f.buildFlowDeleteStrict();
				break;
			case MODIFY:
				b = f.buildFlowModify();
				break;
			case MODIFY_STRICT:
				b = f.buildFlowModifyStrict();
				break;
			default:
				log.warn("Could not decode flow mod command {}", m.getCommand().toString());
				return m;
			}
			b.setMatch(mangleMatch(m.getMatch(), pi, pd))
			.setCookie(m.getCookie())
			.setIdleTimeout(m.getIdleTimeout())
			.setHardTimeout(m.getHardTimeout())
			.setPriority(m.getPriority())
			.setBufferId(m.getBufferId())
			.setOutPort(pi.getPhysicalPortFromProxy(m.getOutPort()))
			.setFlags(m.getFlags())
			.setXid(m.getXid());

			switch (f.getVersion()) {
			case OF_10:
				m = b.setActions(mangleActions(m.getActions(), pi, pd))
				.build();
				break;
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.setTableId(m.getTableId())
				.setCookieMask(m.getCookieMask())
				.setOutGroup(m.getOutGroup())
				.setInstructions(mangleInstructions(m.getInstructions(), pi, pd))
				.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
				break;
			}
			log.info("Flow mod after: {}", m);
			break;
		}
		return m;
	}

	/**
	 * Group mods require we handle the command and the buckets.
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFGroupMod mangleGroupMod(OFGroupMod m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PHYS_TO_PROXY:
			log.error("Cannot send group mod from switch to controller.");
			break;
		case PROXY_TO_PHYS:
			OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			OFGroupMod.Builder b;
			switch (m.getCommand()) {
			case ADD:
				b = f.buildGroupAdd();
				break;
			case DELETE:
				b = f.buildGroupDelete();
				break;
			case MODIFY:
				b = f.buildGroupModify();
				break;
			default:
				log.warn("Could not decode group mod command {}", m.getCommand().toString());
				return m;
			}
			b.setGroup(m.getGroup())
			.setGroupType(m.getGroupType())
			.setBuckets(mangleBuckets(m.getBuckets(), pi, pd))
			.setXid(m.getXid());

			switch (f.getVersion()) {
			case OF_11:
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.build();
				break;
			case OF_10:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
				break;
			}

			break;
		}
		return m;
	}

	/**
	 * Not much to mangle in a features reply except for ports in OF1.0-1.2.
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFFeaturesReply mangleFeaturesReply(OFFeaturesReply m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			log.error("Cannot send features reply from controller to switch.");
			break; 
		case PHYS_TO_PROXY: /* TODO we could learn the DPID here if that's a desired feature */
			OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
			OFFeaturesReply.Builder b = f.buildFeaturesReply();
			b.setCapabilities(m.getCapabilities())
			.setDatapathId(pi.getProxySwitchDpid()) /* no need to check direction for DPID */
			.setNBuffers(m.getNBuffers())
			.setNTables(m.getNTables())
			.setXid(m.getXid());
			switch (f.getVersion()) {
			case OF_10:
				m = b.setActions(m.getActions())
				.setPorts(pi.getProxyPortDescFromPhysical(m.getPorts()))
				.build();
				break;
			case OF_11:
			case OF_12:
				m = b.setPorts(pi.getProxyPortDescFromPhysical(m.getPorts()))
				.setReserved(m.getReserved())
				.build();
				break;
			case OF_13:
			case OF_14:
				m = b.setReserved(m.getReserved())
				.setAuxiliaryId(m.getAuxiliaryId())
				.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			}
			break;
		}
		return m;
	}

	/**
	 * OFPacketIn only requires we change out the in-port and
	 * in-phy-port, which locations vary depending on the
	 * OpenFlow version.
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPacketIn manglePacketIn(OFPacketIn m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			log.error("Cannot send packet in from controller to switch.");
			break;
		case PHYS_TO_PROXY:
			OFFactory f = OFFactories.getFactory(pi.getProxySwitchVersion());
			OFPacketIn.Builder b = f.buildPacketIn();
			b.setBufferId(m.getBufferId())
			.setData(m.getData())
			.setReason(m.getReason())
			.setTableId(m.getTableId())
			.setTotalLen(m.getTotalLen())
			.setXid(m.getXid());
			switch (f.getVersion()) {
			case OF_10:
				/* phy not supported */
				m = b.setInPort(m.getInPort())
				.build();
				break;
			case OF_11:
				m = b.setInPhyPort(pi.getProxyPortFromPhysical(m.getInPhyPort()))
				.setInPort(pi.getProxyPortFromPhysical(m.getInPort()))
				.build();
				break;
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.setCookie(m.getCookie())
				.setMatch(mangleMatch(m.getMatch(), pi, pd))
				.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			}
			break;
		}
		return m;
	}

	/**
	 * Packet out is pretty straightforward. Everything except the actions are in common.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFPacketOut manglePacketOut(OFPacketOut m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			OFFactory f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			OFPacketOut.Builder b = f.buildPacketOut();
			b.setActions(mangleActions(m.getActions(), pi, pd))
			.setBufferId(m.getBufferId())
			.setData(m.getData())
			.setInPort(m.getInPort())
			.setXid(m.getXid());
			switch (f.getVersion()) {
			case OF_10:
			case OF_11:
				m = b.build();
				break;
			case OF_12:
			case OF_13:
			case OF_14:
				m = b.build();
				break;
			default:
				log.error("Unsupported OpenFlow version received from {} for OFMessage {}", pd, m);
			}
			break;
		case PHYS_TO_PROXY:
			log.error("Cannot send packet out from switch to controller.");
			break;
		}
		return m;
	}
	
	/**
	 * TODO INCOMPLETE
	 * 
	 * First function to process any error messages. Helper functions are used for specific
	 * message type processing. This parent function guarantees to the helper functions that the packet
	 * direction is correct prior to invoking them.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFErrorMsg mangleError(OFErrorMsg m, ProxyInfo pi, PacketDirection pd) {
		switch (pd) {
		case PROXY_TO_PHYS:
			log.error("Cannot send error {} from controller to switch.", m.getErrType().toString());
			break;
		case PHYS_TO_PROXY:
			switch (m.getErrType()) {
			case ASYNC_CONFIG_FAILED:
			case BUNDLE_FAILED:
			case EXPERIMENTER:
			case FLOW_MONITOR_FAILED:
			case HELLO_FAILED:
			case METER_MOD_FAILED:
			case ROLE_REQUEST_FAILED:
			case SWITCH_CONFIG_FAILED:
			case TABLE_FEATURES_FAILED:
			case TABLE_MOD_FAILED:
				log.warn("Ignoring error type {}. Message will not be mangled.", m.getErrType().toString());
				break;
			case BAD_ACTION:
				break;
			case BAD_INSTRUCTION:
				break;
			case BAD_MATCH:
				break;
			case BAD_PROPERTY:
				break;
			case BAD_REQUEST:
				break;
			case FLOW_MOD_FAILED:
				break;
			case GROUP_MOD_FAILED:
				break;
			case PORT_MOD_FAILED:
				break;
			case QUEUE_OP_FAILED:
				break;
			default:
				log.error("Unhandled error type {}. Message will not be mangled.", m.getErrType().toString());
				break;
			}
		}
		return m;
	}
	

	/**
	 * Only the output action should be changed, although we might need to change
	 * the others to the appropriate OpenFlow versions. (TODO)
	 * @param al
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFAction> mangleActions(List<OFAction> al, ProxyInfo pi, PacketDirection pd) {
		List<OFAction> na = new ArrayList<OFAction>(al.size());
		for (OFAction a : al) {
			switch (a.getType()) {
			case OUTPUT:
				switch (pd) {
				case PHYS_TO_PROXY:
					na.add(OFFactories.getFactory(pi.getProxySwitchVersion())
							.actions().output(
									pi.getProxyPortFromPhysical(((OFActionOutput)a).getPort()), 
									((OFActionOutput)a).getMaxLen()
									));
					break;
				case PROXY_TO_PHYS:
					na.add(OFFactories.getFactory(pi.getPhysicalSwitchVersion())
							.actions().output(
									pi.getPhysicalPortFromProxy(((OFActionOutput)a).getPort()), 
									((OFActionOutput)a).getMaxLen()
									));
					break;
				}
				break;
			default:
				na.add(a);
				break;
			}
		}
		return na;
	}

	/**
	 * The only instructions we need to mangle are apply and write actions. All others only direct to places
	 * e.g. meter, group that might need to be mangled in different messages e.g. meter/group mod.
	 * @param il
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFInstruction> mangleInstructions(List<OFInstruction> il, ProxyInfo pi, PacketDirection pd) {
		List<OFInstruction> ni = new ArrayList<OFInstruction>(il.size());
		OFFactory f;
		switch (pd) {
		case PHYS_TO_PROXY:
			f = OFFactories.getFactory(pi.getProxySwitchVersion());
			break;
		case PROXY_TO_PHYS:
			f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			break;
		default:
			log.error("Could not determine packet direction {} for ProxyInfo {}", pd.toString(), pi.toString());
			return il;
		}
		for (OFInstruction i : il) {
			switch (i.getType()) {
			case APPLY_ACTIONS:
				ni.add(f.instructions().applyActions(mangleActions(((OFInstructionApplyActions) i).getActions(), pi, pd)));
				break;
			case WRITE_ACTIONS:
				ni.add(f.instructions().applyActions(mangleActions(((OFInstructionWriteActions) i).getActions(), pi, pd)));
				break;
			default:
				ni.add(i);
				break;
			}
		}
		return ni;
	}

	/**
	 * Only need to change the ports. TODO Check for valid versions when converting.
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static Match mangleMatch(Match m, ProxyInfo pi, PacketDirection pd) {
		Match.Builder b;
		OFFactory f;
		switch (pd) {
		case PHYS_TO_PROXY:
			f = OFFactories.getFactory(pi.getProxySwitchVersion());
			break;
		case PROXY_TO_PHYS:
			f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			break;
		default:
			log.error("Could not determine packet direction {} for ProxyInfo {}", pd.toString(), pi.toString());
			return m;
		}
		/* Convert all fields */
		b = MatchUtils.convertToVersion(m, f.getVersion());
		/* Then, replace the ports if they're present */
		b = mangleMatchField(m, pi, pd, b, MatchField.IN_PORT);
		b = mangleMatchField(m, pi, pd, b, MatchField.IN_PHY_PORT);

		return b.build();
	}

	/**
	 * Helper function for {@link OFPacketMangler#mangleMatch(Match, ProxyInfo, PacketDirection)}
	 * Note this only works for OFPort type MatchFields.
	 * @param m
	 * @param pi
	 * @param pd
	 * @param b
	 * @param mf
	 * @return
	 */
	private static Match.Builder mangleMatchField(Match m, ProxyInfo pi, PacketDirection pd, Match.Builder b, MatchField<OFPort> mf) {
		if (b.isExact(mf)) {
			switch (pd) {
			case PHYS_TO_PROXY:
				b.setExact(mf, pi.getProxyPortFromPhysical(m.get(mf)));
				break;
			case PROXY_TO_PHYS:
				b.setExact(mf, pi.getPhysicalPortFromProxy(m.get(mf)));
				break;
			}
		} else if (b.isPartiallyMasked(mf)) {
			switch (pd) {
			case PHYS_TO_PROXY:
				b.setMasked(mf, 
						pi.getProxyPortFromPhysical(m.getMasked(mf).getValue()), 
						m.getMasked(mf).getMask());
				break;
			case PROXY_TO_PHYS:
				b.setMasked(mf, 
						pi.getPhysicalPortFromProxy(m.getMasked(mf).getValue()), 
						m.getMasked(mf).getMask());
				break;
			}
		}
		return b;
	}

	/**
	 * Helper function for converting group buckets.
	 * 
	 * @param m
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static List<OFBucket> mangleBuckets(List<OFBucket> bl, ProxyInfo pi, PacketDirection pd) {
		OFVersion v;
		switch (pd) {
		case PHYS_TO_PROXY:
			v = pi.getProxySwitchVersion();
			break;
		case PROXY_TO_PHYS:
			v = pi.getPhysicalSwitchVersion();
			break;
		default:
			log.error("Could not determine packet direction {} for ProxyInfo {}", pd.toString(), pi.toString());
			return bl;
		}

		List<OFBucket> nbl = new ArrayList<OFBucket>(bl.size());
		for (OFBucket b : bl) {
			OFBucket nb = mangleBucket(b, pi, pd);
			switch (v) {
			case OF_11:
			case OF_12:	
			case OF_13:
			case OF_14:
				nbl.add(nb);
				break;
			case OF_10:
			default:
				log.error("Unsupported OpenFlow version received from {} for OFBucket {}", pd, b);
				break;
			}
		}
		return nbl;
	}

	/**
	 * Helper function for converting a group's bucket.
	 * @param b
	 * @param pi
	 * @param pd
	 * @return
	 */
	private static OFBucket mangleBucket(OFBucket b, ProxyInfo pi, PacketDirection pd) {
		OFFactory f;
		switch (pd) {
		case PHYS_TO_PROXY:
			f = OFFactories.getFactory(pi.getProxySwitchVersion());
			break;
		case PROXY_TO_PHYS:
			f = OFFactories.getFactory(pi.getPhysicalSwitchVersion());
			break;
		default:
			log.error("Could not determine packet direction {} for ProxyInfo {}", pd.toString(), pi.toString());
			return b;
		}
		OFBucket.Builder bb = f.buildBucket();
		bb.setWatchGroup(b.getWatchGroup())
		.setWeight(b.getWeight())
		.setActions(mangleActions(b.getActions(), pi, pd));
		switch (pd) {
		case PHYS_TO_PROXY:
			bb.setWatchPort(pi.getProxyPortFromPhysical(b.getWatchPort()));
			break;
		case PROXY_TO_PHYS:
			bb.setWatchPort(pi.getPhysicalPortFromProxy(b.getWatchPort()));
			break;
		}
		return bb.build();
	}
}