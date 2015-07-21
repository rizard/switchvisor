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

package net.floodlightcontroller.core;

import java.util.List;
import java.util.Map;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.Ethernet;

import org.jboss.netty.util.Timer;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.Controller.ModuleLoaderState;

import net.floodlightcontroller.core.FloodlightContextStore;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
/**
 * The interface exposed by the core bundle that allows you to interact
 * with connected switches.
 *
 * @author David Erickson (daviderickson@cs.stanford.edu)
 */
public interface IFloodlightProviderService extends
        IFloodlightService, Runnable {

    /**
     * A value stored in the floodlight context containing a parsed packet
     * representation of the payload of a packet-in message.
     */
    public static final String CONTEXT_PI_PAYLOAD =
            "net.floodlightcontroller.core.IFloodlightProvider.piPayload";

    /**
     * A FloodlightContextStore object that can be used to retrieve the
     * packet-in payload
     */
    public static final FloodlightContextStore<Ethernet> bcStore =
            new FloodlightContextStore<Ethernet>();

    /**
     * Service name used in the service directory representing
     * the OpenFlow controller-switch channel
     *
     * @see  ILocalServiceAddressTracker
     * @see  IClusterServiceAddressDirectory
     */
    public static final String SERVICE_DIRECTORY_SERVICE_NAME = "openflow";

    /**
     * Adds an OpenFlow message listener
     * @param type The OFType the component wants to listen for
     * @param listener The component that wants to listen for the message
     */
    public void addOFMessageListener(OFType type, IOFMessageListener listener);

    /**
     * Removes an OpenFlow message listener
     * @param type The OFType the component no long wants to listen for
     * @param listener The component that no longer wants to receive the message
     */
    public void removeOFMessageListener(OFType type, IOFMessageListener listener);

    /**
     * Return a non-modifiable list of all current listeners
     * @return listeners
     */
    public Map<OFType, List<IOFMessageListener>> getListeners();

    /**
     * Gets the controller hostname
     * @return the controller hostname
     */
    public String getOFHostname();

    /**
     * Gets the controller's openflow port
     * @return the controller's openflow port
     */
    public int getOFPort();

    /**
     * Run the main I/O loop of the Controller.
     */
    @Override
    public void run();

   /**
    * Return the controller start time in  milliseconds
    * @return
    */
   public long getSystemStartTime();

   /**
    * Get controller memory information
    */
   public Map<String, Long> getMemory();

   /**
    * returns the uptime of this controller.
    * @return
    */
   public Long getUptime();

   public void handleMessage(IOFSwitch sw, OFMessage m,
                          FloodlightContext bContext);

   /**
    * Gets a hash wheeled timer to be used for for timeout scheduling
    * @return a hash wheeled timer
    */
   public Timer getTimer();

   /**
    * Gets the current module loading state.
    * @return the current module loading state.
    */
   ModuleLoaderState getModuleLoaderState();

   /**
    * Gets the current number of worker threads
    * @return Used for netty setup
    */
   public int getWorkerThreads();

}

