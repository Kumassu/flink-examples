package org.example.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Person;

public class TableAPI {


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
    }


    public static void fileToFile() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("gender", DataTypes.STRING());

        Csv csv = new Csv().fieldDelimiter(',');

        tableEnv.connect(new FileSystem().path("D:/tmp/person_src.csv"))
                .withSchema(schema)
                .withFormat(csv)
                .createTemporaryTable("person_src");

        tableEnv.connect(new FileSystem().path("D:/tmp/person_sink.csv"))
                .withSchema(schema)
                .withFormat(csv)
                .inAppendMode()
                .createTemporaryTable("person_sink");

        tableEnv.connect(new FileSystem().path("D:/tmp/person_group_by_gender.csv"))
                .withSchema(new Schema().field("gender", DataTypes.STRING()).field("cnt", DataTypes.BIGINT()))
                .withFormat(csv)
                .inAppendMode()
                // @see org.apache.flink.table.sinks.CsvTableSink
                // do not support upsert or retract mode
                // so can not insert aggregated table to svc file
//                .inUpsertMode()
//                .inRetractMode()
                .createTemporaryTable("person_group_by_gender");

        // Note: you can't convert a aggregated table to a appendStream, user retractStream to allow retract record
        Table groupByGender = tableEnv.from("person_src").groupBy("gender").select("gender, gender.count as cnt");


        // print for test
//        tableEnv.toAppendStream(tableEnv.from("person_src"), Person.class).print();
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
        tableEnv.from("person_src").insertInto("person_sink");

        // will fail
//        groupByGender.insertInto("person_group_by_gender");

//        tableEnv.execute("table api");
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

        Table maleTable = personTable.select("name,age,gender").where("gender = 'MALE'");
        Table femaleTable = personTable.filter("gender = 'MALE'");
        tableEnv.createTemporaryView("male", maleTable);
        tableEnv.createTemporaryView("female", femaleTable);

        DataStream<Person> maleStream = tableEnv.toAppendStream(maleTable, Person.class);
        DataStream<Person> femaleStream = tableEnv.toAppendStream(femaleTable, Person.class);

        maleStream.print("male > ");
        femaleStream.print("female > ");

        env.execute();
    }
}
