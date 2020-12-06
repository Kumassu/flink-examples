package org.example.flink.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.flink.model.KeyedMessage;
import org.example.flink.model.Message;

import java.util.Random;

public class AutoKeyFunction implements MapFunction<Message, KeyedMessage> {

    public static final String KEY_PREFIX = "_partition_";

    private int partitionNum;


    public AutoKeyFunction(int partitionNum) {
        this.partitionNum = partitionNum;
    }


    @Override
    public KeyedMessage map(Message value) throws Exception {
        KeyedMessage keyedMessage = new KeyedMessage();
        keyedMessage.setId(value.getId());
        keyedMessage.setContent(value.getContent());
        keyedMessage.setTimestamp(value.getTimestamp());
        keyedMessage.setPartitionKey(nextKey());
        return keyedMessage;
    }


    private String nextKey() {
        return KEY_PREFIX + (new Random().nextInt(partitionNum) + 1);
    }


}
