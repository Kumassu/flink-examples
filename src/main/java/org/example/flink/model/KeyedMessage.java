package org.example.flink.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class KeyedMessage extends Message {

    private String partitionKey;


    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Override
    public String toString() {
        return "Message{" +
                "timestamp=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(getTimestamp())) +
                ", partitionKey=" + partitionKey +
                '}';
    }

}
