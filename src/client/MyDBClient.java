package client;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;

/**
 * This class should implement your DB client.
 */
public class MyDBClient extends Client {
    private HashMap<String, Callback> callbackMap;
    private NodeConfig<String> nodeConfig = null;

    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        callbackMap = new HashMap<>();
        this.nodeConfig = nodeConfig;
    }

    @Override
    public void callbackSend(InetSocketAddress isa, String request, Callback callback) throws IOException {
        try {
            String key = UUID.randomUUID().toString();
            send(isa, new JSONObject(Map.of("request", request, "key", key)).toString());
            callbackMap.put(key, callback);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleResponse(byte[] bytes, NIOHeader header) {
        try {
            String response = new String(bytes, SingleServer.DEFAULT_ENCODING);
            if (response.startsWith("{")) {
                JSONObject jsonObject = new JSONObject(response);
                String key = jsonObject.getString("key");
                String resp = jsonObject.getString("response");
                if (callbackMap.containsKey(key)) {
                    callbackMap.get(key).handleResponse(resp.getBytes(SingleServer.DEFAULT_ENCODING), header);
                } else {
                    System.out.println(resp);
                }
            } else {
                System.out.println(response);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}