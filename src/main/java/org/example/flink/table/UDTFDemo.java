package org.example.flink.table;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Person;

/**
 * User Defined Table Function
 *
 * 表值函数 one -> multiply
 */
public class UDTFDemo {


    public static class Split extends TableFunction<String> {

        @Override
        public void open(FunctionContext context) {
        }

        public void eval(String str) {
            String[] split = str.split("-");
            for (String s : split) {
                collect(s);
            }
        }

        @Override
        public void close() {
        }
    }



    public static class SplitAsTuple extends TableFunction<Tuple5<String,String,String,String,String>> {

        public void eval(String str) {
            String[] s = str.split("-");
            collect(Tuple5.of(s[0], s[1], s[2], s[3], s[4]));
        }

    }

    public static class SplitAsRow extends TableFunction<Row> {

        public void eval(String str) {
            String[] s = str.split("-");
            Row row = new Row(5);
            for (int i = 0; i < s.length; i++) {
                row.setField(i, s[i]);
            }
            collect(row);
        }

    }



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // table from stream
        DataStreamSource<Person> person = env.addSource(new PersonEmiter());

        Table personTable = tableEnv.fromDataStream(person);
        tableEnv.createTemporaryView("person", personTable);

        tableEnv.createTemporarySystemFunction("SPLIT", new Split());


        var sql = "SELECT p.name, p.age, p.gender, t.a FROM person as p " +
                "left join lateral table(SPLIT(p.name)) as t(a) on true";

        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> personDataStream = tableEnv.toAppendStream(table, Row.class);

        personDataStream.print();
        env.execute();

    }





}
