package server;

import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

import org.json.JSONObject;

import com.datastax.driver.core.*;

/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {
    protected Cluster cluster;
    protected Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        cluster = Cluster.builder()
                .addContactPoint(isaDB.getAddress().getHostAddress())
                .withPort(isaDB.getPort())
                .build();
        session = cluster.connect(keyspace);
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            log.log(Level.INFO, "{0} received message from {1}",
                    new Object[] { this.clientMessenger.getListeningSocketAddress(), header.sndr });
            String reqString = new String(bytes, DEFAULT_ENCODING);
            String key, clientReq;
            if (reqString.startsWith("{")) {
                JSONObject jsonObject = new JSONObject(reqString);
                key = jsonObject.getString("key");
                clientReq = jsonObject.getString("request");
            } else {
                key = UUID.randomUUID().toString();
                clientReq = reqString;
            }
            session.execute(clientReq);
            String response = "Success for message :: " + clientReq;

            this.clientMessenger.send(header.sndr, new JSONObject(Map.of("key", key, "response", response))
                    .toString()
                    .getBytes(DEFAULT_ENCODING));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        super.close();
        session.close();
        cluster.close();
        // TODO: cleanly close anything you created here.
    }
}