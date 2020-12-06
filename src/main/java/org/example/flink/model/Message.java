package org.example.flink.model;

public class Message {


    private String id;
    private long timestamp;
    private Object content;

    public Message() {
    }

    public Message(String id, long timestamp, Object content) {
        this.id = id;
        this.timestamp = timestamp;
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", content=" + content +
                '}';
    }
}
