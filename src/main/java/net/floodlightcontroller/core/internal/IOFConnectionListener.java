package net.floodlightcontroller.core.internal;

import net.floodlightcontroller.core.IOFConnection;
import org.projectfloodlight.openflow.protocol.OFMessage;

public interface IOFConnectionListener {
    void connectionClosed(IOFConnection connection);

    void messageReceived(IOFConnection connection, OFMessage m);
    
    void connectionOpened(IOFConnection connection);	
}
