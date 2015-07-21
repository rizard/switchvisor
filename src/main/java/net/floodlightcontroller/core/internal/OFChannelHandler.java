package net.floodlightcontroller.core.internal;

import java.util.List;

import javax.annotation.Nonnull;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.util.Timer;

import net.floodlightcontroller.core.OFConnection;
import net.floodlightcontroller.core.internal.OpenflowPipelineFactory.PipelineHandler;
import net.floodlightcontroller.core.internal.OpenflowPipelineFactory.PipelineHandshakeTimeout;

import org.projectfloodlight.openflow.protocol.OFMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Channel handler deals with the switch connection and dispatches
 *  messages to the higher orders of control.
 * @author Jason Parraga <Jason.Parraga@Bigswitch.com>
 * @author Ryan Izard <ryan.izard@bigswitch.com>
 */
class OFChannelHandler extends IdleStateAwareChannelHandler {

	private static final Logger log = LoggerFactory.getLogger(OFChannelHandler.class);

	private final ChannelPipeline pipeline;
	private final IOFConnectionListener connectionListener;
	private Channel channel;
	private final Timer timer;
	private volatile OFConnection connection;

	/**
	 * Creates a handler for interacting with the switch channel
	 * 
	 * @param connectionListener
	 *            the class that listens for OF connections (controller)
	 * @param pipeline
	 *            the channel pipeline
	 * @param threadPool
	 *            the thread pool
	 * @param idleTimer
	 *            the hash wheeled timer used to send idle messages (echo).
	 *            passed to constructor to modify in case of aux connection.
	 */
	OFChannelHandler(@Nonnull IOFConnectionListener connectionListener,
			@Nonnull ChannelPipeline pipeline,
			@Nonnull Timer timer) {

		Preconditions.checkNotNull(connectionListener, "connectionListener");
		Preconditions.checkNotNull(pipeline, "pipeline");
		Preconditions.checkNotNull(timer, "timer");

		this.pipeline = pipeline;
		this.connectionListener = connectionListener;
		this.timer = timer;

		log.debug("constructor on OFChannelHandler {}", String.format("%08x", System.identityHashCode(this)));
	}
	
	/*
	 * IdleStateAwareChannelHandler Implementation
	 */
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		log.debug("channelConnected on OFChannelHandler {}", String.format("%08x", System.identityHashCode(this)));
		channel = e.getChannel();
		log.info("New switch connection from {}", channel.getRemoteAddress());
		
		notifyConnectionOpened(connection);
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		// Only handle cleanup connection is even known
		if(this.connection != null){
			// Alert the connection object that the channel has been disconnected
			this.connection.disconnected();
			// Punt the cleanup to the Switch Manager
			notifyConnectionClosed(this.connection);
		}
		log.info("Disconnected connection");
	}

	@Override
	public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception {
		log.debug("channelIdle on OFChannelHandler {}", String.format("%08x", System.identityHashCode(this)));
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		if (e.getMessage() instanceof List) {
			@SuppressWarnings("unchecked")
			List<OFMessage> msglist = (List<OFMessage>)e.getMessage();
			for (OFMessage ofm : msglist) {
				try {
					// Do the actual packet processing
					sendMessageToConnection(ofm);
				}
				catch (Exception ex) {
					// We are the last handler in the stream, so run the
					// exception through the channel again by passing in
					// ctx.getChannel().
					Channels.fireExceptionCaught(ctx.getChannel(), ex);
				}
			}
		}
		else {
			Channels.fireExceptionCaught(ctx.getChannel(),
					new AssertionError("Message received from channel is not a list"));
		}
	}

	/**
	 * Notifies the channel listener that we have a valid baseline connection
	 */
	private final void notifyConnectionOpened(OFConnection connection){
		this.connection = connection;
		this.connectionListener.connectionOpened(connection);
	}

	/**
	 * Notifies the channel listener that our connection has been closed
	 */
	private final void notifyConnectionClosed(OFConnection connection){
		connection.getListener().connectionClosed(connection);
	}

	/**
	 * Passes a message to the channel listener
	 */
	private final void sendMessageToConnection(OFMessage m) {
		connection.messageReceived(m);
	}

	/**
	 * Sets the channel pipeline's handshake timeout to a more appropriate value
	 * for the remainder of the connection.
	 */
	private void setSwitchHandshakeTimeout() {

		HandshakeTimeoutHandler handler = new HandshakeTimeoutHandler(
				this,
				this.timer,
				PipelineHandshakeTimeout.SWITCH);

		pipeline.replace(PipelineHandler.CHANNEL_HANDSHAKE_TIMEOUT,
				PipelineHandler.SWITCH_HANDSHAKE_TIMEOUT, handler);
	}
}
