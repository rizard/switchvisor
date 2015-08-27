package net.floodlightcontroller.switchvisor.web;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.floodlightcontroller.firewall.IFirewallService;
import net.floodlightcontroller.switchvisor.ISwitchVisorService;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.TableId;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;

public class SwitchVisorAddProxyResource extends ServerResource {
	private static final Logger log = LoggerFactory.getLogger(SwitchVisorAddProxyResource.class);
	
	
	/**
	 * Expect:
	 * {
	 * 		"physical-dpid"		:	"dpid", // TODO optional? -- can be learned
	 * 		"proxy-dpid"		:	"dpid",
	 * 		"controller-ip"		:	"ipv4-address",
	 * 		"controler-tcp-port":	"tcp-port",
	 * 		"controller-vlan"	:	"vlan-id",
	 * 		"port-mapping"		:	[
	 * 									{
	 * 										"physical-switch-port"	:	"switch-port",
	 * 										"proxy-switch-port"		:	"switch-port"
	 * 									},
	 * 									{
	 * 										"physical-switch-port"	:	"switch-port",
	 * 										"proxy-switch-port"		:	"switch-port"
	 * 									},
	 * 									...
	 * 									...
	 * 								]
	 * }
	 * 
	 * @param json, JSON string formatted in the aforementioned manner with all required information
	 * @return a JSON-defined status update informing the result of the operation
	 */
	@Post("json")
	public Map<String, String> addProxy(String json) {
		MappingJsonFactory f = new MappingJsonFactory();
		JsonParser jp;
		Map<String, String> retValue = new HashMap<String, String>();

		if (json == null || json.isEmpty()) {
			return retValue;
		}

		try {
			try {
				jp = f.createParser(json);
			} catch (JsonParseException e) {
				throw new IOException(e);
			}

			jp.nextToken();
			if (jp.getCurrentToken() != JsonToken.START_OBJECT) {
				throw new IOException("Expected START_OBJECT");
			}

			while (jp.nextToken() != JsonToken.END_OBJECT) {
				if (jp.getCurrentToken() != JsonToken.FIELD_NAME) {
					throw new IOException("Expected FIELD_NAME");
				}

				String n = jp.getCurrentName();
				jp.nextToken();
				if (jp.getText().equals("")) {
					continue;
				}

				DatapathId dpid;
				try {
					n = n.trim();
					dpid = DatapathId.of(n);

					TableId tablesToGetDefaultFlow;
					String value = jp.getText();
					if (value != null && !value.isEmpty()) {
						value = value.trim().toLowerCase();
						try {
							tablesToGetDefaultFlow = TableId.of(
									value.startsWith("0x") 
									? Integer.parseInt(value.replaceFirst("0x", ""), 16) 
											: Integer.parseInt(value)
									); /* will throw exception if outside valid TableId number range */
							//retValue.put(dpid, tablesToGetDefaultFlow);
							log.info("Setting max tables to receive table-miss flow to {} for DPID {}", 
									tablesToGetDefaultFlow.toString(), dpid.toString());
						} catch (IllegalArgumentException e) { /* catches both IllegalArgumentExcpt. and NumberFormatExcpt. */
							log.error("Invalid value of {} for max tables to receive table-miss flow for DPID {}. Using default of {}.", value, dpid.toString());
						}
					}
				} catch (NumberFormatException e) {
					log.error("Invalid DPID format {} for max tables to receive table-miss flow for specific DPID. Using default for the intended DPID.", n);
				}
			}
		} catch (IOException e) {
			log.error("Using default for remaining DPIDs. JSON formatting error in max tables to receive table-miss flow for DPID input String: {}", e);
		}
		return retValue;
	}

}
