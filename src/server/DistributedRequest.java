package server;

public class DistributedRequest {
    public String message;
    public String messageId;
    public String serverId;
    public String messageType;
    public int seqNumber;
    public int lamportClock;

    public DistributedRequest(String message,
            String messageId,
            String serverId,
            String messageType,
            int seqNumber,
            int lamportClock) {
        this.message = message;
        this.messageId = messageId;
        this.serverId = serverId;
        this.messageType = messageType;
        this.seqNumber = seqNumber;
        this.lamportClock = lamportClock;
    }

    public String getMessage(){
        return this.message;
    }
    public String getMessageId(){
        return this.messageId;
    }
    public String getMessageType(){
        return this.messageType;
    }
    public String getServerId(){
        return this.serverId;
    }
    public int getSeqNumber(){
        return this.seqNumber;
    }
    public int getLamportClock(){
        return this.lamportClock;
    }
}
