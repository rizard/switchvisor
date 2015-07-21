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

import org.projectfloodlight.openflow.protocol.OFMessage;

/**
 * An interface to describe classes that write OF messages.
 * E.g. IOFSwitch, IOFConnection
 */

public interface IOFMessageWriter{

    /**
     * Writes to the OFMessage to the output stream.
     *
     * <p><b>Note:</b> this method has fire-and-forget semantics. When the connection is
     * not currently connected, it will silently discard the messages.
     *
     * @param m
     */
    void write(OFMessage m);

    /**
     * Writes the list of messages to the output stream.
     *
     * <p><b>Note:</b> this method has fire-and-forget semantics. When the connection is
     * not currently connected, it will silently discard the messages.
     *
     * @param msglist
     */
    void write(Iterable<OFMessage> msglist);
}
