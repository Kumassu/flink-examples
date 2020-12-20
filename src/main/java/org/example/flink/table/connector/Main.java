package org.example.flink.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)" +
                "WITH (" +
                "  'connector' = 'socket'," +
                "  'hostname' = 'localhost'," +
                "  'port' = '9999'," +
                "  'byte-delimiter' = '10'," +
                "  'format' = 'changelog-csv'," +
                "  'changelog-csv.column-delimiter' = ','" +
                ")");

        tableEnv.executeSql("CREATE TABLE UserScoreSink (name STRING, score INT)" +
                "WITH (" +
                "  'connector' = 'socket'," +
                "  'hostname' = 'aaaa'," +
                "  'port' = 'aaaa'" +
                ")");


        Table userScores = tableEnv.from("UserScores");

        userScores.executeInsert("UserScoreSink");


//        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(userScores, Row.class);
//        tuple2DataStream.print();

//        env.execute();
        tableEnv.execute("table");
        // INSERT,Alice,12
        // INSERT,Bob,5
        // DELETE,Alice,12
        // INSERT,Alice,18


    }


}
