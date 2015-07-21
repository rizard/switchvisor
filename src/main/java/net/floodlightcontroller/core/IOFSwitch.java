/**
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

import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.types.DatapathId;

import com.google.common.collect.ImmutableList;

/**
 * An openflow switch connecting to the controller.  This interface offers
 * methods for interacting with switches using OpenFlow, and retrieving
 * information about the switches.
 */
public interface IOFSwitch extends IOFMessageWriter {
    /**
     * Disconnect all the switch's channels and mark the switch as disconnected
     */
    void disconnect();

    /**
     * Get the IP address of the remote (switch) end of the connection
     * @return the inet address
     */
    SocketAddress getInetAddress();

    /**
     * Check is switch is connected
     * @return Whether or not this switch is connected
     */
    boolean isConnected();

    /**
     * Retrieves the date the switch connected to this controller
     * @return the date
     */
    Date getConnectedSince();

    /**
     * Get the datapathId of the switch
     * @return
     */
    DatapathId getId();

    /**
     * Returns a factory object that can be used to create OpenFlow messages.
     * @return
     */
    OFFactory getOFFactory();

    /**
     * Gets the OF connections for this switch instance
     * @return Collection of IOFConnection
     */
    ImmutableList<IOFConnection> getConnections();
    
    /**
     * Add a new switch connection
     * @param connection
     */
	public void registerConnection(IOFConnection connection);

	/**
	 * Remove an old switch connection
	 * @param connection
	 */
	public void removeConnection(IOFConnection connection);
}
