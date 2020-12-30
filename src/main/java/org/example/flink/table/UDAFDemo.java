package org.example.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Person;

import java.util.LinkedList;
import java.util.List;

/**
 * User Defined Aggregation Function
 *
 * 聚合函数 multiply -> one
 */
public class UDAFDemo {


    public static class AvgAge extends AggregateFunction<Double, List<Integer>> {

        // 初始化count UDAF的accumulator。
        public List<Integer> createAccumulator() {
            return new LinkedList<>();
        }

        // getValue提供了如何通过存放状态的accumulator计算count UDAF的结果的方法。
        public Double getValue(List<Integer> ages) {
            return ages.stream().mapToInt(p -> p).summaryStatistics().getAverage();
        }

        // accumulate提供了如何根据输入的数据更新count UDAF存放状态的accumulator。
        public void accumulate(List<Integer> ages, Integer age) {
            ages.add(age);
        }

        public void merge(List<Integer> accumulator, Iterable<List<Integer>> its) {
            for (List<Integer> other : its) {
                accumulator.addAll(other);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Person> person = env.addSource(new PersonEmiter());

        Table personTable = tableEnv.fromDataStream(person);
        tableEnv.createTemporaryView("person", personTable);
        tableEnv.registerFunction("AVG_AGE", new AvgAge());

        var sql = "SELECT gender, AVG_AGE(age) FROM person group by gender";
        Table myUDFTable = tableEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> stream = tableEnv.toRetractStream(myUDFTable, Row.class);

        stream.print();
        env.execute();
    }




}
