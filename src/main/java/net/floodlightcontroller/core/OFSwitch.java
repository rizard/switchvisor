/**
 *    Copyright 2012, Big Switch Networks, Inc.
 *    Originally created by David Erickson, Stanford University
 *
 *    Licensed under the Apache License, Version 2.0 (the "License"); you may
 *    not use this file except in compliance with the License. You may obtain
 *    a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
 **/

package net.floodlightcontroller.core;

import java.net.SocketAddress;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.projectfloodlight.openflow.protocol.OFActionType;
import org.projectfloodlight.openflow.protocol.OFCapabilities;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFAuxId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * This is the internal representation of an openflow switch.
 */
public class OFSwitch implements IOFSwitch {
	protected static final Logger log =
			LoggerFactory.getLogger(OFSwitch.class);

	/* Switch features from initial featuresReply */
	protected Set<OFCapabilities> capabilities;
	protected long buffers;
	protected Set<OFActionType> actions;
	protected final DatapathId datapathId;

	private final Map<OFAuxId, IOFConnection> connections;
	protected OFFactory factory;

	/**
	 * Members hidden from subclasses
	 */
	private volatile boolean connected;

	public OFSwitch(IOFConnection connection, @Nonnull OFFactory factory,
			@Nonnull DatapathId datapathId) {
		if(connection == null)
			throw new NullPointerException("connection must not be null");
		if(!connection.getAuxId().equals(OFAuxId.MAIN))
			throw new IllegalArgumentException("connection must be the main connection");
		if(factory == null)
			throw new NullPointerException("factory must not be null");

		this.connected = true;
		this.factory = factory;
		this.datapathId = datapathId;

		// Connections
		this.connections = new ConcurrentHashMap<OFAuxId, IOFConnection>();
		this.connections.put(connection.getAuxId(), connection);
	}

	@Override
	public OFFactory getOFFactory() {
		return factory;
	}

	@Override
	public void registerConnection(IOFConnection connection) {
		this.connections.put(connection.getAuxId(), connection);
	}


	@Override
	public ImmutableList<IOFConnection> getConnections() {
		return ImmutableList.<IOFConnection> copyOf(this.connections.values());
	}

	@Override
	public void removeConnection(IOFConnection connection) {
		this.connections.remove(connection.getAuxId());
	}

	@Override
	public void write(OFMessage m) {
		log.trace("Channel: {}, Connected: {}", connections.get(OFAuxId.MAIN).getRemoteInetAddress(), connections.get(OFAuxId.MAIN).isConnected());
		connections.get(OFAuxId.MAIN).write(m);
	}

	/**
	 * Gets a connection specified by aux Id.
	 * @param auxId the specified aux id for the connection desired.
	 * @return the aux connection specified by the auxId
	 */
	public IOFConnection getConnection(OFAuxId auxId) {
		IOFConnection connection = this.connections.get(auxId);
		if(connection == null){
			throw new IllegalArgumentException("OF Connection for " + this + " with " + auxId + " does not exist.");
		}
		return connection;
	}

	@Override
	public void write(Iterable<OFMessage> msglist) {
		connections.get(OFAuxId.MAIN).write(msglist);
	}

	@Override
	public void disconnect() {

		// Iterate through connections and perform cleanup
		for(Entry<OFAuxId, IOFConnection> entry : this.connections.entrySet()){
			entry.getValue().disconnect();
			this.connections.remove(entry.getKey());
		}
		log.debug("~~~~~~~SWITCH DISCONNECTED~~~~~~");
		// Remove all counters from the store
		connected = false;
	}

	@Override
	public DatapathId getId() {
		if (datapathId == null)
			throw new RuntimeException("Features reply has not yet been set");
		return datapathId;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "OFSwitchBase DPID[" + ((datapathId != null) ? datapathId.toString() : "?") + "]";
	}

	@Override
	public Date getConnectedSince() {
		return this.connections.get(OFAuxId.MAIN).getConnectedSince();
	}	

	/**
	 * Get the IP Address for the switch
	 * @return the inet address
	 */
	@Override
	public SocketAddress getInetAddress() {
		return connections.get(OFAuxId.MAIN).getRemoteInetAddress();
	}

	@Override
	public boolean isConnected() {
		return connected;
	}
}
