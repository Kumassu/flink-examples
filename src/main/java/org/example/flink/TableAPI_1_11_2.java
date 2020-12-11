package org.example.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Gender;
import org.example.flink.model.Person;

import static org.apache.flink.table.api.Expressions.$;


public class TableAPI_1_11_2 {



    public static void createEnv() {
        ExecutionEnvironment batchExecEnv = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // old planner for batch
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchExecEnv);


        // old planner for stream
        EnvironmentSettings olderPlannerStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default")
                .withBuiltInDatabaseName("default")
                .build();
        StreamTableEnvironment env = StreamTableEnvironment.create(streamExecEnv, olderPlannerStreamSettings);


        // blink planner for batch
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .withBuiltInCatalogName("default")
                .withBuiltInDatabaseName("default")
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);
    }


    public static void main(String[] args) throws Exception {
//        streamToTableToStream();
        fileToFile();
    }


    public static void tableFromConnector() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // blink planner for streaming
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default")
                .withBuiltInDatabaseName("default")
                .build();


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        TableResult tableResult = tableEnv.executeSql("CREATE TABLE Person (`name` STRING, age INT, gender STRING)");



    }


    public static void fileToFile() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE PERSON_SRC " +
                "(" +
                "`name` STRING, `age` INT, `gender` STRING" +
                ") " +
                "with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'file:///D:/tmp/person_src.csv'," +
                "'format' = 'csv'" +
                ")");

        tableEnv.executeSql("CREATE TABLE PERSON_SINK " +
                "(" +
                "`name` STRING, `age` INT, `gender` STRING" +
                ") " +
                "with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'file:///D:/tmp/person_sink.csv'," +
                "'format' = 'csv'" +
                ")");

        tableEnv.executeSql("CREATE TABLE PERSON_GROUP_BY_GENDER (`gender` STRING, `cnt` BIGINT)");



        // Note: you can't convert a aggregated table to a appendStream, user retractStream to allow retract record
        Table groupByGender = tableEnv.from("PERSON_SRC").groupBy($("gender")).select($("gender"), $("gender").count().as("cnt"));


        // print for test
//        tableEnv.toAppendStream(tableEnv.from("PERSON_SRC"), Person.class).print();
//        tableEnv.toRetractStream(groupByGender, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})).print();
        // the print results :
        // A true {@link Boolean} flag indicates an add message, a false flag indicates a retract message.
        // @see StreamTableEnvironment#toRetractStream
        // 7> Person{name='jerry', age=20, gender=male}
        // 3> Person{name='cat', age=18, gender=female}
        // 1> Person{name='dog', age=30, gender=male}
        // 6> Person{name='tom', age=20, gender=male}
        // 4> (true,(female,1))
        // 4> (true,(male,1))
        // 4> (false,(male,1))
        // 4> (true,(male,2))
        // 4> (false,(male,2))
        // 4> (true,(male,3))


        // sink to file
        tableEnv.from("PERSON_SRC").executeInsert("PERSON_SINK");

        env.execute();
    }


    public static void streamToTableToStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // blink planner for streaming
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default")
                .withBuiltInDatabaseName("default")
                .build();


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // table from stream
        DataStreamSource<Person> person = env.addSource(new PersonEmiter());

        Table personTable = tableEnv.fromDataStream(person);
        tableEnv.createTemporaryView("person", personTable);

        Table maleTable = personTable.select($("name"), $("age"), $("gender")).where($("gender").isEqual(Gender.MALE));
        Table femaleTable = personTable.filter($("gender").isEqual(Gender.FEMALE));
        tableEnv.createTemporaryView("male", maleTable);
        tableEnv.createTemporaryView("female", femaleTable);

        DataStream<Person> maleStream = tableEnv.toAppendStream(maleTable, Person.class);
        DataStream<Person> femaleStream = tableEnv.toAppendStream(femaleTable, Person.class);

        maleStream.print("male > ");
        femaleStream.print("female > ");

        env.execute();
    }

}
