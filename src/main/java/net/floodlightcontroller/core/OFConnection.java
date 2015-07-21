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
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.Timer;

import java.util.Date;

import net.floodlightcontroller.core.internal.Controller;
import net.floodlightcontroller.core.internal.IOFConnectionListener;
import net.floodlightcontroller.debugcounter.IDebugCounterService;

import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFAuxId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * Implementation of an openflow connection to switch. Encapsulates a
 * {@link Channel}, and provides message write and request/response handling
 * capabilities.
 *
 * @author Andreas Wundsam <andreas.wundsam@bigswitch.com>
 */
public class OFConnection implements IOFConnection {
    private static final Logger logger = LoggerFactory.getLogger(OFConnection.class);
    private final DatapathId dpid;
    private final OFFactory factory;
    private final Channel channel;
    private final OFAuxId auxId;

    private final Date connectedSince;

    protected final static ThreadLocal<List<OFMessage>> localMsgBuffer =
            new ThreadLocal<List<OFMessage>>();

    private IOFConnectionListener listener;

    public OFConnection(@Nonnull DatapathId dpid,
                        @Nonnull OFFactory factory,
                        @Nonnull Channel channel,
                        @Nonnull OFAuxId auxId,
                        @Nonnull IDebugCounterService debugCounters,
                        @Nonnull Timer timer) {
        Preconditions.checkNotNull(dpid, "dpid");
        Preconditions.checkNotNull(factory, "factory");
        Preconditions.checkNotNull(channel, "channel");
        Preconditions.checkNotNull(timer, "timer");
        Preconditions.checkNotNull(debugCounters);

        this.listener = NullConnectionListener.INSTANCE;
        this.dpid = dpid;
        this.factory = factory;
        this.channel = channel;
        this.auxId = auxId;
        this.connectedSince = new Date();
    }

    @Override
    public void write(OFMessage m) {
        if (!isConnected()) {
            if (logger.isDebugEnabled())
                logger.debug("{}: not connected - dropping message {}", this, m);
            return;
        }
        if (logger.isDebugEnabled())
            logger.debug("{}: send {}", this, m);
        List<OFMessage> msgBuffer = localMsgBuffer.get();
        if (msgBuffer == null) {
            msgBuffer = new ArrayList<OFMessage>();
            localMsgBuffer.set(msgBuffer);
        }

        msgBuffer.add(m);

        if ((msgBuffer.size() >= Controller.BATCH_MAX_SIZE) || ((m.getType() != OFType.PACKET_OUT) && (m.getType() != OFType.FLOW_MOD))) {
            this.write(msgBuffer);
            localMsgBuffer.set(null);
        }
    }

    @Override
    public void write(Iterable<OFMessage> msglist) {
        if (!isConnected()) {
            if (logger.isDebugEnabled())
                logger.debug(this.toString() + " : not connected - dropping {} element msglist {} ",
                        Iterables.size(msglist),
                        String.valueOf(msglist).substring(0, 80));
            return;
        }
        for (OFMessage m : msglist) {
            if (logger.isTraceEnabled())
                logger.trace("{}: send {}", this, m);
        }
        this.channel.write(msglist);
    }

    // Notifies the connection object that the channel has been disconnected
    public void disconnected() {

    }

    @Override
    public void disconnect() {
        this.channel.disconnect();
    }

    @Override
    public String toString() {
        String channelString = (channel != null) ? String.valueOf(channel.getRemoteAddress()): "?";
        return "OFConnection [" + getDatapathId() + "(" + getAuxId() + ")" + "@" + channelString + "]";
    }

    @Override
    public Date getConnectedSince() {
        return connectedSince;
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    public SocketAddress getRemoteInetAddress() {
        return channel.getRemoteAddress();
    }

    @Override
    public SocketAddress getLocalInetAddress() {
        return channel.getLocalAddress();
    }

    @Override
    public DatapathId getDatapathId() {
        return dpid;
    }

    @Override
    public OFAuxId getAuxId() {
        return auxId;
    }
    
    @Override
    public OFFactory getOFFactory() {
        return this.factory;
    }

    public IOFConnectionListener getListener() {
        return listener;
    }

    /** set the connection listener
     *  <p>
     *  Note: this is assumed to be called from the Connection's IO Thread.
     *
     * @param listener
     */
    @Override
    public void setListener(IOFConnectionListener listener) {
        this.listener = listener;
    }

    public void messageReceived(OFMessage m) {
        listener.messageReceived(this, m);
    }

    /** A dummy connection listener that just logs warn messages. Saves us a few null checks
     * @author Andreas Wundsam <andreas.wundsam@bigswitch.com>
     */
    private static class NullConnectionListener implements IOFConnectionListener {
        public final static NullConnectionListener INSTANCE = new NullConnectionListener();

        private NullConnectionListener() { }

        @Override
        public void connectionClosed(IOFConnection connection) {
            logger.warn("NullConnectionListener for {} - received connectionClosed", connection);
        }

        @Override
        public void messageReceived(IOFConnection connection, OFMessage m) {
            logger.warn("NullConnectionListener for {} - received messageReceived: {}", connection, m);
        }

		@Override
		public void connectionOpened(IOFConnection connection) {
			logger.warn("NullConnectionListener for {} - received connectionOpened", connection);
		}
    }
}