package edu.stevens.cs549.dhts.main;

import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBElement;

import org.glassfish.jersey.jackson.JacksonFeature;

import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.resource.TableRep;
import edu.stevens.cs549.dhts.resource.TableRow;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	/*
	 * Encapsulate Web client operations here.
	 * 
	 * TODO: Fill in missing operations.
	 */

	/*
	 * Creation of client instances is expensive, so just create one.
	 */
	protected Client client;
	

	public WebClient() {
		client = ClientBuilder.newBuilder()
				.register(ObjectMapperProvider.class)
				.register(JacksonFeature.class)
				.build();
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	private Response getRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.get();
			return cr;
		} catch (Exception e) {
			error("Exception during GET request", e);
			return null;
		}
	}

	private Response putRequest(URI uri, TableRep tableRep) {
		// TODO Complete.
		try {
			Response cr = client.target(uri)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.put(Entity.json(tableRep));
			return cr;
		} catch (Exception e) {
			error("Exception during PUT request", e);
			return null;
		}
		// throw new IllegalStateException("Unimplemented putRequest");
	}

	private Response putRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request()
					.put(Entity.text(""));
			return cr;
		} catch (Exception e) {
			error("Exception during PUT request", e);
			return null;
		}
	}


	/*
	 * Ping a remote site to see if it is still available.
	 */
	public boolean isFailed(URI base) {
		URI uri = UriBuilder.fromUri(base).path("info").build();
		Response c = getRequest(uri);
		return c.getStatus() >= 300;
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getPred(NodeInfo node) throws DHTBase.Failed {
		URI predPath = UriBuilder.fromUri(node.addr).path("pred").build();
		info("client getPred(" + predPath + ")");
		Response response = getRequest(predPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /pred");
		} else {
			NodeInfo pred = response.readEntity(NodeInfo.class);
			return pred;
		}
	}

	public NodeInfo getSucc(NodeInfo node) throws DHTBase.Failed {
		URI succPath = UriBuilder.fromUri(node.addr).path("succ").build();
		info("client getSucc(" + succPath + ")");
		Response response = getRequest(succPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /succ");
		} else {
			NodeInfo succ = response.readEntity(NodeInfo.class);
			return succ;
		}
	}	


	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public TableRep notify(NodeInfo node, TableRep predDb) throws DHTBase.Failed {
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
		NodeInfo thisNode = predDb.getInfo();
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("notify");
		URI notifyPath = ub.queryParam("id", thisNode.id).build();
		info("client notify(" + notifyPath + ")");
		Response response = putRequest(notifyPath, predDb);
		if (response != null && response.getStatusInfo() == Response.Status.NOT_MODIFIED) {
			/*
			 * Do nothing, the successor did not accept us as its predecessor.
			 */
			return null;
		} else if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("PUT /notify?id=ID");
		} else {
			TableRep bindings = response.readEntity(TableRep.class);
			return bindings;
		}
	}

	// TODO
	/*
	 * Get bindings under a key.	
	 */
	public String[] get(NodeInfo node, String skey) throws DHTBase.Failed {
		// throw new IllegalStateException("Unimplemented get");
		
		UriBuilder ub = UriBuilder.fromUri(node.addr).path(skey);
		URI getPath = ub.queryParam("key", skey).build();
		info("client getBindings(" + getPath + ")");
		Response response = getRequest(getPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET ?key=KEY");
		} else {
			return response.readEntity(TableRow.class).vals;

		}
		
	}

	// TODO 
	/*
	 * Put bindings under a key.
	 */
    public void add(NodeInfo node, String skey, String v) throws DHTBase.Failed {
		// throw new IllegalStateException("Unimplemented add");

    	URI addPath = UriBuilder.fromUri(node.addr).queryParam("key",skey).queryParam("value", v).build();
		info("client add(" + addPath + ")");
		Response response = putRequest(addPath);

		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed(response + "PUT ?key=" + skey + "&val=" + v);
		}
	}
	
	// TODO 
	/*
	 * Delete bindings under a key.
	 */
	public void delete(NodeInfo node, String skey, String v) throws DHTBase.Failed {
		// throw new IllegalStateException("Unimplemented delete");

		URI deletePath = UriBuilder.fromUri(node.addr).queryParam("key",skey).queryParam("value", v).build();
		info("client delete(" + deletePath + ")");
		Response response = deleteRequest(deletePath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed(response + "PUT ?key=" + skey + "&val=" + v);
		}
	}

	private Response deleteRequest(URI deletePath) {
		// TODO Auto-generated method stub
		try {
			Response cr = client.target(deletePath)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.delete();
			return cr;
		} catch (Exception e) {
			error("Exception during Delete request", e);
			return null;
		}
	}

	// TODO 
	/*
	 * Find successor of an id. Used by join protocol
	 */
	public NodeInfo findSuccessor(URI addr, int id) throws DHTBase.Failed {
		// throw new IllegalStateException("Unimplemented findSuccessor");

        URI findPath = UriBuilder.fromUri(addr).path("find").queryParam("id", id).build();
		info("client findSuccessor(" + findPath + ")");
		Response response = getRequest(findPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /find");
		} else {
			return response.readEntity(NodeInfo.class);
		}
	}

	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) throws DHTBase.Failed {
		URI getPath = UriBuilder.fromUri(node.addr).path("finger").queryParam("id",id).build();
		info("client findClosestfinger(" + getPath + ")");
		Response response = getRequest(getPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /finger");
		} else {
			NodeInfo finger = response.readEntity(NodeInfo.class);
			return finger;
		}
	}
	
}
