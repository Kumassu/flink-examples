package org.example.flink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.example.flink.util.ConcurrentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Demo source : https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/state/broadcast_state.html
 */
public class BroadcastStateDemo {


    private static final Logger log = LoggerFactory.getLogger(BroadcastStateDemo.class);


    static class Item {
        Color color;
        Shape shape;

        public Item(Color color, Shape shape) {
            this.color = color;
            this.shape = shape;
        }

        @Override
        public String toString() {
            return "Item{" + "color=" + color + ", shape=" + shape + '}';
        }
    }

    enum Color {
        RED,
        BLUE,
        WHITE,
    }

    enum Shape {
        A,
        B,
        C
    }

    static class Rule {
        String name;
        Shape hold;
        Shape release;

        public Rule(String name, Shape hold, Shape release) {
            this.name = name;
            this.hold = hold;
            this.release = release;
        }

        @Override
        public String toString() {
            return "Rule{" + "name='" + name + '\'' + ", hold=" + hold + ", release=" + release + '}';
        }
    }



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Item> itemStream = env.addSource(new SourceFunction<Item>() {

            private boolean cancel = false;

            @Override
            public void run(SourceContext<Item> ctx) {
                while (!cancel) {
                    Shape[] shapes = Shape.values();
                    Shape randomShape = shapes[new Random().nextInt(shapes.length)];
                    Color[] colors = Color.values();
                    Color randomColor = colors[new Random().nextInt(colors.length)];
                    Item item = new Item(randomColor, randomShape);

                    log.info("[Item Source] emit item: {}", item);
                    ctx.collect(item);
                    ConcurrentUtils.sleep(TimeUnit.SECONDS, 1);
                }
            }

            @Override
            public void cancel() {
                cancel = true;
            }
        });


        DataStreamSource<Rule> ruleStream = env.addSource(new SourceFunction<Rule>() {

            private boolean cancel = false;

            @Override
            public void run(SourceContext<Rule> ctx) {
                while (!cancel) {
                    Stream.of(
                            new Rule("A-B", Shape.A, Shape.B),
                            new Rule("B-C", Shape.B, Shape.C),
                            new Rule("C-A", Shape.C, Shape.A))
                    .forEach(rule -> {
                        log.info("[Rule Source] emit rule: {}", rule);
                        ctx.collect(rule);
                        ConcurrentUtils.sleep(TimeUnit.SECONDS, 10);
                    });
                    ConcurrentUtils.sleep(TimeUnit.SECONDS, Integer.MAX_VALUE);
                }
            }

            @Override
            public void cancel() {
                cancel = true;
            }
        });

        // a map descriptor to store the name of the rule (string) and the rule itself.
        final MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {}));

        // broadcast the rules and create the broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream.broadcast(ruleStateDescriptor);

        // item stream key by color,
        // rule pattern, A - B : [A, A ... B], B - C : [B, B ... C]
        // store all elements in state until pattern matches
        SingleOutputStreamOperator<String> matchedStream = itemStream
                .keyBy((KeySelector<Item, String>) value -> value.color.name())
                .connect(ruleBroadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, Item, Rule, String>() {

                    // store partial matches, i.e. first elements of the pair waiting for their second element
                    // we keep a list as we may have many first elements waiting
                    private final MapStateDescriptor<String, List<Item>> mapStateDesc =
                            new MapStateDescriptor<>(
                                    "items",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    new ListTypeInfo<>(Item.class));

                    // identical to our ruleStateDescriptor above
                    private final MapStateDescriptor<String, Rule> ruleStateDescriptor =
                            new MapStateDescriptor<>(
                                    "RulesBroadcastState",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    TypeInformation.of(new TypeHint<Rule>() {}));

                    @Override
                    public void processElement(Item value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc);
                        final Shape shape = value.shape;
                        Iterable<Map.Entry<String, Rule>> rules = ctx.getBroadcastState(ruleStateDescriptor).immutableEntries();

                        for (Map.Entry<String, Rule> entry : rules) {
                            final String ruleName = entry.getKey();
                            final Rule rule = entry.getValue();

                            List<Item> stored = state.get(ruleName);
                            if (stored == null) {
                                stored = new ArrayList<>();
                            }

                            if (shape == rule.release && !stored.isEmpty()) {
                                out.collect("holds: " + stored + ", release: " + value);
                                stored.clear();
                            }

                            // there is no else{} to cover if rule.first == rule.second
                            if (shape.equals(rule.hold)) {
                                stored.add(value);
                            }

                            if (stored.isEmpty()) {
                                state.remove(ruleName);
                            } else {
                                state.put(ruleName, stored);
                            }
                        }

                        log.info("[Process Element] currentKey: {}, rule state:{}, item state: {}",
                                ctx.getCurrentKey(), rules, state.entries());
                    }

                    @Override
                    public void processBroadcastElement(Rule value, Context ctx, Collector<String> out) throws Exception {
                        log.info("[Broadcast] rule: {}", value);
                        ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
                    }
                });


        matchedStream.print();

        env.execute();

    }


}
