/**
*    Copyright 2011, Big Switch Networks, Inc.
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

package net.floodlightcontroller.core.internal;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IListener.Command;
import net.floodlightcontroller.core.IOFConnection;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.FloodlightModuleLoader;
import net.floodlightcontroller.core.util.ListenerDispatcher;
import net.floodlightcontroller.debugcounter.IDebugCounterService;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFType;

import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.perfmon.IPktInProcessingTimeService;
import net.floodlightcontroller.threadpool.IThreadPoolService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * The main controller class.  Handles all setup and network listeners
 */
public class Controller implements IFloodlightProviderService, IOFConnectionListener {

    protected static final Logger log = LoggerFactory.getLogger(Controller.class);

    protected ConcurrentMap<OFType, ListenerDispatcher<OFType,IOFMessageListener>> messageListeners;
    protected ControllerCounters counters;
    protected Timer timer;
    
    /* Module Loader State */
    private ModuleLoaderState moduleLoaderState;
    public enum ModuleLoaderState {
        INIT, STARTUP, COMPLETE
    }

    /* Module dependencies */
    private IDebugCounterService debugCounterService;
    private IPktInProcessingTimeService pktinProcTimeService;
    private IThreadPoolService threadPoolService;

    /* Configuration options */
    protected int openFlowPort = 6653; // new registered OF port number
    private String openFlowHostname = null;
    protected int workerThreads = 0;
    
    /* Performance related configuration */
    protected static final int SEND_BUFFER_SIZE = 128 * 1024;
    public static final int BATCH_MAX_SIZE = 1;
    protected static final boolean ALWAYS_DECODE_ETH = true;
    
    /* TLS stuff */
    private static boolean useSsl = false;
    private static String keyStore = null;
    private static String keyStorePassword = null;

    /*
     * IOFConnectionListener Implementation
     */
    
    @Override
    public ModuleLoaderState getModuleLoaderState(){
        return this.moduleLoaderState;
    }
    
	@Override
	public void connectionClosed(IOFConnection connection) {
		log.info("Controller has received a new connection closed message for {}", connection.getRemoteInetAddress());
	}

	@Override
	public void messageReceived(IOFConnection connection, OFMessage m) {
		log.info("Controller has received a new message for {}", connection.getRemoteInetAddress());
	}

	@Override
	public void connectionOpened(IOFConnection connection) {
		log.info("Controller has received a new connection for {}", connection.getRemoteInetAddress());
	}
                
    /*
     * Getters and Setters for Controller
     */

    void setDebugCounter(IDebugCounterService debugCounters) {
        this.debugCounterService = debugCounters;
    }

    IDebugCounterService getDebugCounter() {
        return this.debugCounterService;
    }

    void setPktInProcessingService(IPktInProcessingTimeService pits) {
        this.pktinProcTimeService = pits;
    }

    void setThreadPoolService(IThreadPoolService tp) {
        this.threadPoolService = tp;
    }
    IThreadPoolService getThreadPoolService() {
        return this.threadPoolService;
    }

    /*
     * IFloodlightProviderService Implementation
     */
    
    /**
     * Tell controller that we're ready to accept switches loop
     * @throws IOException
     */
    @Override
    public void run() {
        this.moduleLoaderState = ModuleLoaderState.COMPLETE;

        if (log.isDebugEnabled()) {
            logListeners();
        }
        
        log.info("Looping forever");

        while (true);
    }
    
    @Override
    public int getWorkerThreads() {
        return this.workerThreads;
    }
    
    @Override
    public long getSystemStartTime() {
        RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
        return rb.getStartTime();
    }

    @Override
    public Map<String, Long> getMemory() {
        Map<String, Long> m = new HashMap<String, Long>();
        Runtime runtime = Runtime.getRuntime();
        m.put("total", runtime.totalMemory());
        m.put("free", runtime.freeMemory());
        return m;
    }

    @Override
    public Long getUptime() {
        RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
        return rb.getUptime();
    }

    @Override
    public Timer getTimer() {
        return this.timer;
    }
    
    @Override
    public String getOFHostname() {
        return openFlowHostname;
    }
    @Override
    public int getOFPort() {
        return openFlowPort;
    }

    @Override
    public synchronized void addOFMessageListener(OFType type, IOFMessageListener listener) {
        ListenerDispatcher<OFType, IOFMessageListener> ldd =
            messageListeners.get(type);
        if (ldd == null) {
            ldd = new ListenerDispatcher<OFType, IOFMessageListener>();
            messageListeners.put(type, ldd);
        }
        ldd.addListener(type, listener);
    }

    @Override
    public synchronized void removeOFMessageListener(OFType type, IOFMessageListener listener) {
        ListenerDispatcher<OFType, IOFMessageListener> ldd =
            messageListeners.get(type);
        if (ldd != null) {
            ldd.removeListener(listener);
        }
    }
    
    @Override
    public Map<OFType, List<IOFMessageListener>> getListeners() {
        Map<OFType, List<IOFMessageListener>> lers =
            new HashMap<OFType, List<IOFMessageListener>>();
        for(Entry<OFType, ListenerDispatcher<OFType, IOFMessageListener>> e : messageListeners.entrySet()) {
            lers.put(e.getKey(), e.getValue().getOrderedListeners());
        }
        return Collections.unmodifiableMap(lers);
    }
    
    /**
     *
     * Handle and dispatch a message to IOFMessageListeners.
     *
     * We only dispatch messages to listeners if the controller's role is MASTER.
     *
     * @param sw The switch sending the message
     * @param m The message the switch sent
     * @param flContext The Floodlight context to use for this message. If
     * null, a new context will be allocated.
     * @throws IOException
     */
    @Override
    public void handleMessage(IOFSwitch sw, OFMessage m,
                                 FloodlightContext bContext) {
        Ethernet eth = null;
        log.trace("Dispatching OFMessage to listeners.");

        counters.dispatchMessage.increment();

        switch (m.getType()) {
            case PACKET_IN:
            	counters.packetIn.increment();
                OFPacketIn pi = (OFPacketIn)m;

                if (pi.getData().length <= 0) {
                    log.error("Ignoring PacketIn (Xid = " + pi.getXid() + ") because the data field is empty.");
                    return;
                }

                if (Controller.ALWAYS_DECODE_ETH) {
                    eth = new Ethernet();
                    eth.deserialize(pi.getData(), 0, pi.getData().length);
                }
                // fall through to default case...

            default:

                List<IOFMessageListener> listeners = null;
                if (messageListeners.containsKey(m.getType())) {
                    listeners = messageListeners.get(m.getType()).getOrderedListeners();
                }

                FloodlightContext bc = null;
                if (listeners != null) {
                    // Check if floodlight context is passed from the calling
                    // function, if so use that floodlight context, otherwise
                    // allocate one
                    if (bContext == null) {
                        bc = flcontext_alloc();
                    } else {
                        bc = bContext;
                    }
                    if (eth != null) {
                        IFloodlightProviderService.bcStore.put(bc,
                                IFloodlightProviderService.CONTEXT_PI_PAYLOAD,
                                eth);
                    }

                    // Get the starting time (overall and per-component) of
                    // the processing chain for this packet if performance
                    // monitoring is turned on
                    pktinProcTimeService.bootstrap(listeners);
                    pktinProcTimeService.recordStartTimePktIn();
                    Command cmd;
                    for (IOFMessageListener listener : listeners) {
                        pktinProcTimeService.recordStartTimeComp(listener);
                        cmd = listener.receive(sw, m, bc);
                        pktinProcTimeService.recordEndTimeComp(listener);

                        if (Command.STOP.equals(cmd)) {
                            break;
                        }
                    }
                    pktinProcTimeService.recordEndTimePktIn(sw, m, bc);
                } else {
                    if (m.getType() != OFType.BARRIER_REPLY)
                        log.warn("Unhandled OF Message: {} from {}", m, sw);
                    else
                        log.debug("Received a Barrier Reply, no listeners for it");
                }

                if ((bContext == null) && (bc != null)) flcontext_free(bc);
        }
    }
    
    /*
     * Controller Helper Functions
     */
    
    /**
     * flcontext_cache - Keep a thread local stack of contexts
     */
    protected static final ThreadLocal<Stack<FloodlightContext>> flcontext_cache =
        new ThreadLocal <Stack<FloodlightContext>> () {
            @Override
            protected Stack<FloodlightContext> initialValue() {
                return new Stack<FloodlightContext>();
            }
        };

    /**
     * flcontext_alloc - pop a context off the stack, if required create a new one
     * @return FloodlightContext
     */
    protected static FloodlightContext flcontext_alloc() {
        FloodlightContext flcontext = null;

        if (flcontext_cache.get().empty()) {
            flcontext = new FloodlightContext();
        }
        else {
            flcontext = flcontext_cache.get().pop();
        }

        return flcontext;
    }

    /**
     * flcontext_free - Free the context to the current thread
     * @param flcontext
     */
    protected void flcontext_free(FloodlightContext flcontext) {
        flcontext.getStorage().clear();
        flcontext_cache.get().push(flcontext);
    }

    private void logListeners() {
        for (Map.Entry<OFType, ListenerDispatcher<OFType, IOFMessageListener>> entry : messageListeners.entrySet()) {
            OFType type = entry.getKey();
            ListenerDispatcher<OFType, IOFMessageListener> ldd = entry.getValue();

            StringBuilder sb = new StringBuilder();
            sb.append("OFListeners for ");
            sb.append(type);
            sb.append(": ");
            for (IOFMessageListener l : ldd.getOrderedListeners()) {
                sb.append(l.getName());
                sb.append(",");
            }
            log.debug(sb.toString());
        }
    }

    public void removeOFMessageListeners(OFType type) {
        messageListeners.remove(type);
    }
    
    private void setConfigParams(Map<String, String> configParams) throws FloodlightModuleException {
        String ofPort = configParams.get("openflowPort");
        if (!Strings.isNullOrEmpty(ofPort)) {
            try {
                this.openFlowPort = Integer.parseInt(ofPort);
            } catch (NumberFormatException e) {
                log.error("invalid openflow port specifier", e);
                throw new FloodlightModuleException("invalid port specifier in cofig");
            }
            log.debug("OpenFlow port set to {}", this.openFlowPort);
        }

        String threads = configParams.get("workerthreads");
        if (!Strings.isNullOrEmpty(threads)) {
            this.workerThreads = Integer.parseInt(threads);
        }
        log.debug("Number of worker threads set to {}", this.workerThreads);
        
        /* 
		 * Get SSL config.
		 * 
		 * If a password is blank, the password field may or may not be specified.
		 * If it is specified, an empty string will be expected for blank.
		 * 
		 * The path MUST be specified if SSL is enabled.
		 */
		String path = configParams.get("keyStorePath");
		String pass = configParams.get("keyStorePassword");
		String useSsl = configParams.get("useSsl");

		if (useSsl == null || path == null || path.isEmpty() || 
				(!useSsl.equalsIgnoreCase("yes") && !useSsl.equalsIgnoreCase("true") &&
						!useSsl.equalsIgnoreCase("yep") && !useSsl.equalsIgnoreCase("ja") &&
						!useSsl.equalsIgnoreCase("stimmt")
						)
				) {
			log.warn("SSL disabled. Using unsecure connections between Floodlight and switches.");
			Controller.useSsl = false;
			Controller.keyStore = null;
			Controller.keyStorePassword = null;
		} else {
			log.info("SSL enabled. Using secure connections between Floodlight and switches.");
			log.info("SSL keystore path: {}, password: {}", path, (pass == null ? "" : pass)); 
			Controller.useSsl = true;
			Controller.keyStore = path;
			Controller.keyStorePassword = (pass == null ? "" : pass);
		}

    }

    /**
     * Initialize internal data structures
     */
    public void init(Map<String, String> configParams) throws FloodlightModuleException {

        this.moduleLoaderState = ModuleLoaderState.INIT;

        // These data structures are initialized here because other
        // module's startUp() might be called before ours        
        this.messageListeners = new ConcurrentHashMap<OFType, ListenerDispatcher<OFType, IOFMessageListener>>();
       
        setConfigParams(configParams);

        this.timer = new HashedWheelTimer();

        this.counters = new ControllerCounters(debugCounterService);
        
        /*
         * Set up the proxy-to-switch server socket. We will
         * listen for incoming switch connections here.
         * 
         * Messages received will be dispatched to any
         * IOFConnectionListeners by OFChannelHandler.
         */
        bootstrapNetty();
     }

    /**
     * Startup all of the controller's components
     * @param floodlightModuleLoader
     */
    public void startupComponents(FloodlightModuleLoader floodlightModuleLoader) throws FloodlightModuleException {
        this.moduleLoaderState = ModuleLoaderState.STARTUP;
    }

    public ControllerCounters getCounters() {
        return this.counters;
    }
    
    /**
	 * Bootstraps Netty, the server that handles all OpenFlow connections
	 */
	public void bootstrapNetty() {
		try {
			final ServerBootstrap bootstrap = createServerBootStrap();

			bootstrap.setOption("reuseAddr", true);
			bootstrap.setOption("child.keepAlive", true);
			bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.sendBufferSize", Controller.SEND_BUFFER_SIZE);
			
			ChannelPipelineFactory pfact = useSsl ? new OpenflowPipelineFactory(this.getTimer(), this /*some new conn listener here*/, keyStore, keyStorePassword) :
				new OpenflowPipelineFactory(this.getTimer(), this);
			
			bootstrap.setPipelineFactory(pfact);
			InetSocketAddress sa = new InetSocketAddress(this.getOFPort());
			final ChannelGroup cg = new DefaultChannelGroup();
			cg.add(bootstrap.bind(sa));

			log.info("Listening for switch connections on {}", sa);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Helper for bootstrapNetty.
	 * @return
	 */
	private ServerBootstrap createServerBootStrap() {
		if (this.getWorkerThreads() == 0) {
			return new ServerBootstrap(
					new NioServerSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()));
		} else {
			return new ServerBootstrap(
					new NioServerSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool(), this.getWorkerThreads()));
		}
	}
}