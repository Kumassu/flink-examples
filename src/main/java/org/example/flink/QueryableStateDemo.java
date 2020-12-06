package org.example.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

/**
 *
 * Source from: https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/state/queryable_state.html
 *
 * 1. Activating Queryable State
 * To enable queryable state on your Flink cluster, you need to do the following:
 * - copy the flink-queryable-state-runtime_2.11-1.11.2.jar from the opt/ folder
 *   of your Flink distribution, to the lib/ folder.
 * - set the property queryable-state.enable to true.
 * - To verify that your cluster is running with queryable state enabled,
 *   check the logs of any task manager for the line: "Started the Queryable State Proxy Server @ ..."
 *
 *
 * 2. Two ways for setting state to be visible to the outside world:
 * - either a QueryableStateStream, a convenience object which acts as a sink and offers its
 *   incoming values as queryable state, or
 * - the stateDescriptor.setQueryable(String queryableStateName) method, which makes the keyed
 *   state represented by the state descriptor,queryable.
 *
 *
 * 3. Configuration
 *
 * The following configuration parameters influence the behaviour of the queryable state server and client.
 * They are defined in QueryableStateOptions.
 *
 * State Server
 * - queryable-state.server.ports: the server port range of the queryable state server.
 *   This is useful to avoid port clashes if more than 1 task managers run on the same machine.
 *   The specified range can be: a port: “9123”, a range of ports: “50100-50200”, or a list of
 *   ranges and or points: “50100-50200,50300-50400,51234”. The default port is 9067.
 * - queryable-state.server.network-threads: number of network (event loop) threads receiving
 *   incoming requests for the state server (0 => #slots)
 * - queryable-state.server.query-threads: number of threads handling/serving incoming requests
 *   for the state server (0 => #slots).
 *
 * Proxy
 * - queryable-state.proxy.ports: the server port range of the queryable state proxy.
 *   This is useful to avoid port clashes if more than 1 task managers run on the same machine.
 *   The specified range can be: a port: “9123”, a range of ports: “50100-50200”, or a list of
 *   ranges and or points: “50100-50200,50300-50400,51234”. The default port is 9069.
 * - queryable-state.proxy.network-threads: number of network (event loop) threads receiving
 *   incoming requests for the client proxy (0 => #slots)
 * - queryable-state.proxy.query-threads: number of threads handling/serving incoming requests
 *   for the client proxy (0 => #slots).
 *
 *
 * 4. Architecture
 * The Queryable State feature consists of three main entities:
 *
 * the QueryableStateClient, which (potentially) runs outside the Flink cluster and submits the
 * user queries,
 * the QueryableStateClientProxy, which runs on each TaskManager (i.e. inside the Flink cluster)
 * and is responsible for receiving the client’s queries, fetching the requested state from the
 * responsible Task Manager on his behalf, and returning it to the client, and
 * the QueryableStateServer which runs on each TaskManager and is responsible for serving the
 * locally stored state.
 * The client connects to one of the proxies and sends a request for the state associated with a
 * specific key, k. As stated in Working with State, keyed state is organized in Key Groups, and
 * each TaskManager is assigned a number of these key groups. To discover which TaskManager is
 * responsible for the key group holding k, the proxy will ask the JobManager. Based on the answer,
 * the proxy will then query the QueryableStateServer running on that TaskManager for the state
 * associated with k, and forward the response back to the client.
 *
 */
public class QueryableStateDemo {

    private static Logger logger = LoggerFactory.getLogger(QueryableStateDemo.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> personStream = env.addSource(new PersonEmiter());


        personStream.keyBy(person -> person.gender.name())
                .map(new RichMapFunction<Person, Person>() {

                    private transient ValueState<Integer> count;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("count",
                                TypeInformation.of(new TypeHint<Integer>() {}));
                        descriptor.setQueryable("queryable-state-peron-count");
                        count = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Person map(Person value) throws Exception {
                        if (count.value() == null) {
                            count.update(0);
                        }
                        count.update(count.value() + 1);
                        logger.info("person: {}, count: {}", value, count.value());
                        return value;
                    }
                });

                // below using  asQueryableState() to make all the incoming elements as queryable state
//                .keyBy(person -> person.gender.name())
//                .asQueryableState("queryable-state-peron-count",
//                        new ValueStateDescriptor<>("person-count",
//                        TypeInformation.of(new TypeHint<Person>() {})));

        personStream.print();
        env.execute();
    }

    public static class Client {

        public static void main(String[] args) throws UnknownHostException {
            QueryableStateClient client = new QueryableStateClient("localhost", 9069);

            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableForceAvro();
            executionConfig.disableForceKryo();
            client.setExecutionConfig(executionConfig);

            // the state descriptor of the state to be fetched.
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("count", TypeInformation.of(new TypeHint<Integer>() {}));

            CompletableFuture<ValueState<Integer>> resultFuture = client.getKvState(
                    JobID.fromHexString("f493ff0f4ac51cd470b33c4ae07c7657"),
                    "queryable-state-peron-count",
                    "MALE",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    descriptor);

            try {
                ValueState<Integer> valueState = resultFuture.get();
                logger.info("query result: {}", valueState.value());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }



}
