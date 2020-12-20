package org.example.flink.table.connector;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class MyTableSink implements DynamicTableSink {


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).addContainedKind(RowKind.DELETE).build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new SinkFunction<RowData>() {
            @Override
            public void invoke(RowData value, Context context) throws Exception {
                System.out.println("sink row : " + value);
            }
        });
    }

    @Override
    public DynamicTableSink copy() {
        return new MyTableSink();
    }

    @Override
    public String asSummaryString() {
        return "print";
    }
}