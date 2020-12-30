package org.example.flink.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Person;

/**
 * User Defined Function
 * 标量函数 one -> one
 */
public class UDFDemo {

    public static class MyUDF extends ScalarFunction {

        public long eval(String key) {
            return key.hashCode();
        }

    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Person> person = env.addSource(new PersonEmiter());

        Table personTable = tableEnv.fromDataStream(person);
        tableEnv.createTemporaryView("person", personTable);
        tableEnv.createTemporarySystemFunction("HASH", new MyUDF());


        var sql = "SELECT name, age, gender, HASH(name) FROM person";

        Table myUDFTable = tableEnv.sqlQuery(sql);
        DataStream<Row> personDataStream = tableEnv.toAppendStream(myUDFTable, Row.class);

        personDataStream.print();
        env.execute();

    }
}
