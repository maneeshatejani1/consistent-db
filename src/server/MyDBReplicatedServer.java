package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.logging.Level;

import org.json.JSONObject;

/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {
    protected final MessageNIOTransport<String, String> serverMessenger;
    protected final String myID;

    protected int seqNumber;
    protected int lamportClock;

    protected PriorityQueue<DistributedRequest> messageQueue;

    protected Map<String, Integer> seqNumTracker;
    protected Map<String, PriorityQueue<DistributedRequest>> bufferPerServer;
    protected Map<String, Integer> ackCounter;

    public static final String SERVER_PREFIX = "server.";
    public static final int SERVER_PORT_OFFSET = 1000;

    private HashMap<String, NIOHeader> clientRequests;

    public class MqComparator implements Comparator<DistributedRequest> {

        @Override
        public int compare(DistributedRequest dr1, DistributedRequest dr2) {
            if (dr1.lamportClock > dr2.lamportClock)
                return 1;
            else if (dr1.lamportClock < dr2.lamportClock)
                return -1;
            else {
                int result = dr1.serverId.compareTo(dr2.serverId);
                if (result < 0)
                    return 1;
                else if (result > 0)
                    return -1;
                else
                    return 0;
            }
        }
    }

    public class SqComparator implements Comparator<DistributedRequest> {
        @Override
        public int compare(DistributedRequest dr1, DistributedRequest dr2) {
            if (dr1.seqNumber > dr2.seqNumber)
                return 1;
            else if (dr1.seqNumber < dr2.seqNumber)
                return -1;
            return 0;
        }
    }

    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
            InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;
        this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
                new AbstractBytePacketDemultiplexer() {
                    @Override
                    public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                        handleMessageFromServer(bytes, nioHeader);
                        return true;
                    }
                }, true);
        this.seqNumber = 0;
        this.lamportClock = 0;
        this.messageQueue = new PriorityQueue<DistributedRequest>(new MqComparator());
        this.seqNumTracker = new HashMap<>();
        this.bufferPerServer = new HashMap<>();
        this.ackCounter = new HashMap<>();
        this.clientRequests = new HashMap<>();
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
            if (!node.equals(myID)) {
                this.seqNumTracker.put(node, 0);
                this.bufferPerServer.put(node, new PriorityQueue<DistributedRequest>(new SqComparator()));
            }
        }
        log.log(Level.INFO, "Server {0} started on {1}",
                new Object[] { this.myID, this.clientMessenger.getListeningSocketAddress() });
    }

    private void multicastMsg(DistributedRequest request) {
        JSONObject jsonObject = new JSONObject(request);
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
            if (!node.equals(myID)) {
                try {
                    this.serverMessenger.send(node, jsonObject.toString().getBytes(DEFAULT_ENCODING));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // TODO: process bytes received from clients here
    @Override
    protected synchronized void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            String reqString = new String(bytes, DEFAULT_ENCODING);
            String messageId, clientReq;
            if (reqString.startsWith("{")) {
                JSONObject jsonObject = new JSONObject(reqString);
                messageId = jsonObject.getString("key");
                clientReq = jsonObject.getString("request");
            } else {
                messageId = UUID.randomUUID().toString();
                clientReq = reqString;
            }
            this.clientRequests.put(messageId, header);
            this.lamportClock = this.lamportClock + 1;
            this.seqNumber = this.seqNumber + 1;
            DistributedRequest distributedRequest = new DistributedRequest(clientReq, messageId, myID,
                    "NON_ACK", this.seqNumber, this.lamportClock);
            this.messageQueue.add(distributedRequest);

            multicastMsg(distributedRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void executeMsg(DistributedRequest polledRequest) {
        this.session.execute(polledRequest.message);
    }

    private boolean isInOrder(DistributedRequest request) {
        return this.seqNumTracker.get(request.serverId) == request.seqNumber - 1;
    }

    private void updateAckCounter(DistributedRequest request) {
        if (!this.ackCounter.containsKey(request.messageId)) {
            this.ackCounter.put(request.messageId, 0);
        }
        this.ackCounter.put(request.messageId, this.ackCounter.get(request.messageId) + 1);
    }

    private void updateSequenceNumber(DistributedRequest request) {
        this.seqNumTracker.put(request.serverId, this.seqNumTracker.get(request.serverId) + 1);
    }

    private void handleNonAckServerMsg(DistributedRequest requestFromServer) {
        this.messageQueue.add(requestFromServer);
        this.lamportClock = Math.max(this.lamportClock, requestFromServer.lamportClock + 1);
        this.seqNumber = this.seqNumber + 1;
        this.lamportClock = this.lamportClock + 1;
        DistributedRequest ackRequest = new DistributedRequest(requestFromServer.message,
                requestFromServer.messageId,
                this.myID,
                "ACK",
                this.seqNumber,
                this.lamportClock);
        updateAckCounter(requestFromServer);
        multicastMsg(ackRequest);
        executeQueue();
    }

    private void executeQueue() {
        while (!messageQueue.isEmpty() && this.ackCounter.containsKey(messageQueue.peek().messageId) &&
                this.ackCounter
                        .get(messageQueue.peek().messageId) == (this.serverMessenger.getNodeConfig().getNodeIDs().size()
                                - 1)) {
            DistributedRequest polledRequest = messageQueue.poll();
            executeMsg(polledRequest);
            if (clientRequests.containsKey(polledRequest.messageId)) {
                try {
                    JSONObject jsonObject = new JSONObject(Map.of("key", polledRequest.messageId,
                            "response", "Success for message :: " + polledRequest.message));
                    clientMessenger.send(clientRequests.get(polledRequest.messageId).sndr,
                            jsonObject.toString().getBytes(DEFAULT_ENCODING));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleAckServerMsg(DistributedRequest requestFromServer) {
        this.lamportClock = Math.max(this.lamportClock, requestFromServer.lamportClock + 1);
        updateAckCounter(requestFromServer);
    }

    // TODO: process bytes received from servers here
    protected synchronized void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        try {
            JSONObject jsonObject = new JSONObject(new String(bytes, DEFAULT_ENCODING));
            DistributedRequest requestFromServer = new DistributedRequest(jsonObject.getString("message"),
                    jsonObject.getString("messageId"), jsonObject.getString("serverId"),
                    jsonObject.getString("messageType"), jsonObject.getInt("seqNumber"),
                    jsonObject.getInt("lamportClock"));
            if (isInOrder(requestFromServer)) {
                this.seqNumTracker.put(requestFromServer.serverId,
                        this.seqNumTracker.get(requestFromServer.serverId) + 1);
                if ("NON_ACK".equals(requestFromServer.messageType)) {
                    handleNonAckServerMsg(requestFromServer);
                } else {
                    handleAckServerMsg(requestFromServer);
                    if (this.messageQueue.stream().anyMatch(x -> x.messageId.equals(requestFromServer.messageId))) {
                        executeQueue();
                    }
                }

                while (!bufferPerServer.get(requestFromServer.serverId).isEmpty() &&
                        bufferPerServer.get(requestFromServer.serverId).peek().seqNumber == seqNumTracker
                                .get(requestFromServer.serverId) + 1) {
                    DistributedRequest peekedRequest = bufferPerServer.get(requestFromServer.serverId).peek();
                    if ("NON_ACK".equals(peekedRequest.messageType)) {
                        handleNonAckServerMsg(bufferPerServer.get(requestFromServer.serverId).poll());
                        this.updateSequenceNumber(requestFromServer);
                    } else {
                        handleAckServerMsg(bufferPerServer.get(requestFromServer.serverId).poll());
                        this.updateSequenceNumber(requestFromServer);
                        if (this.messageQueue.stream().anyMatch(x -> x.messageId.equals(peekedRequest.messageId))) {
                            executeQueue();
                        }
                    }
                }
            } else {
                this.bufferPerServer.get(requestFromServer.serverId).add(requestFromServer);
            }
            log.log(Level.INFO, "{0} received relayed message from {1}",
                    new Object[] { this.myID, header.sndr }); // simply log
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {
        super.close();
        this.serverMessenger.stop();
    }
}