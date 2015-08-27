package net.floodlightcontroller.switchvisor.web;

import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.routing.Router;

import net.floodlightcontroller.restserver.RestletRoutable;

public class SwitchVisorRoutable implements RestletRoutable{

	@Override
	public Restlet getRestlet(Context context) {
		Router router = new Router(context);
        router.attach("/add-proxy/json", SwitchVisorAddProxyResource.class);

        return router;
	}

	@Override
	public String basePath() {
		return "/wm/switchvisor";
	}

}
