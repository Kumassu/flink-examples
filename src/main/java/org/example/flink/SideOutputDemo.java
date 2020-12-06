package org.example.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Person;


/**
 * from : https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/side_output.html
 *
 * Side Outputs
 * In addition to the main stream that results from DataStream operations, you can also produce
 * any number of additional side output result streams. The type of data in the result streams
 * does not have to match the type of data in the main stream and the types of the different
 * side outputs can also differ. This operation can be useful when you want to split a stream
 * of data where you would normally have to replicate the stream and then filter out from each
 * stream the data that you don’t want to have.
 *
 * When using side outputs, you first need to define an OutputTag that will be used to identify
 * a side output stream:
 *{@code
 * // this needs to be an anonymous inner class, so that we can analyze the type
 * OutputTag<String> outputTag = new OutputTag<String>("side-output") {};
 * }
 * Notice how the OutputTag is typed according to the type of elements that the side output stream contains.
 *
 * Emitting data to a side output is possible from the following functions:
 * {@link ProcessFunction}
 * {@link KeyedProcessFunction}
 * {@link CoProcessFunction}
 * {@link KeyedCoProcessFunction}
 * {@link ProcessWindowFunction}
 * {@link ProcessAllWindowFunction}
 *
 * You can use the Context parameter, which is exposed to users in the above functions, to emit data to a
 * side output identified by an OutputTag. Here is an example of emitting side output data from a ProcessFunction:
 * {@code
 * DataStream<Integer> input = ...;
 *
 * final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
 *
 * SingleOutputStreamOperator<Integer> mainDataStream = input
 *   .process(new ProcessFunction<Integer, Integer>() {
 *
 *       @Override
 *       public void processElement(
 *           Integer value,
 *           Context ctx,
 *           Collector<Integer> out) throws Exception {
 *         // emit data to regular output
 *         out.collect(value);
 *
 *         // emit data to side output
 *         ctx.output(outputTag, "sideout-" + String.valueOf(value));
 *       }
 *     });
 * }
 * For retrieving the side output stream you use getSideOutput(OutputTag) on the result of the DataStream operation.
 * This will give you a DataStream that is typed to the result of the side output stream:
 * {@code
 * final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
 *
 * SingleOutputStreamOperator<Integer> mainDataStream = ...;
 *
 * DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag);
 * }
 */
public class SideOutputDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> personStream = env.addSource(new PersonEmiter());

        SingleOutputStreamOperator<Person> availablePersonStream = personStream.process(new ProcessFunction<Person, Person>() {

            private OutputTag<Person> underAgeTag = new OutputTag<>("under ages", TypeInformation.of(Person.class));

            private OutputTag<Person> theAged = new OutputTag<>("the aged", TypeInformation.of(Person.class));

            @Override
            public void processElement(Person value, Context ctx, Collector<Person> out) throws Exception {
                if (value.age < 18) {
                    ctx.output(underAgeTag, value);
                } else if (value.age > 60) {
                    ctx.output(theAged, value);
                } else {
                    out.collect(value);
                }
            }
        });

        DataStream<Person> underAgesStream = availablePersonStream.getSideOutput(new OutputTag<>("under ages",  TypeInformation.of(Person.class)));
        DataStream<Person> theAgedStream = availablePersonStream.getSideOutput(new OutputTag<>("the aged",  TypeInformation.of(Person.class)));
        theAgedStream.print(">> the aged: ");
        underAgesStream.print(">> under ages: ");
        availablePersonStream.print(">> available: ");

        env.execute();
    }

}
