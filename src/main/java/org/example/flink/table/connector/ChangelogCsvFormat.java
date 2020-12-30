package org.example.flink.table.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private final String columnDelimiter;

    public ChangelogCsvFormat(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context,
            DataType producedDataType) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo = (TypeInformation<RowData>) context.createTypeInformation(
                producedDataType);

        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the endpackage org.example.flink.table.connector;
        //
        //import org.apache.flink.api.common.serialization.DeserializationSchema;
        //import org.apache.flink.api.common.typeinfo.TypeInformation;
        //import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
        //import org.apache.flink.configuration.Configuration;
        //import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
        //import org.apache.flink.table.data.RowData;
        //
        //import java.io.ByteArrayOutputStream;
        //import java.io.InputStream;
        //import java.net.InetSocketAddress;
        //import java.net.Socket;
        //
        //public class SocketSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
        //
        //    private final String hostname;
        //    private final int port;
        //    private final byte byteDelimiter;
        //    private final DeserializationSchema<RowData> deserializer;
        //
        //    private volatile boolean isRunning = true;
        //    private Socket currentSocket;
        //
        //    public SocketSourceFunction(String hostname, int port, byte byteDelimiter, DeserializationSchema<RowData> deserializer) {
        //        this.hostname = hostname;
        //        this.port = port;
        //        this.byteDelimiter = byteDelimiter;
        //        this.deserializer = deserializer;
        //    }
        //
        //    @Override
        //    public TypeInformation<RowData> getProducedType() {
        //        return deserializer.getProducedType();
        //    }
        //
        //    @Override
        //    public void open(Configuration parameters) throws Exception {
        //        deserializer.open(() -> getRuntimeContext().getMetricGroup());
        //    }
        //
        //    @Override
        //    public void run(SourceContext<RowData> ctx) throws Exception {
        //        while (isRunning) {
        //            // open and consume from socket
        //            try (final Socket socket = new Socket()) {
        //                currentSocket = socket;
        //                socket.connect(new InetSocketAddress(hostname, port), 0);
        //                try (InputStream stream = socket.getInputStream()) {
        //                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        //                    int b;
        //                    while ((b = stream.read()) >= 0) {
        //                        // buffer until delimiter
        //                        if (b != byteDelimiter) {
        //                            buffer.write(b);
        //                        }
        //                        // decode and emit record
        //                        else {
        //                            ctx.collect(deserializer.deserialize(buffer.toByteArray()));
        //                            buffer.reset();
        //                        }
        //                    }
        //                }
        //            } catch (Throwable t) {
        //                t.printStackTrace(); // print and continue
        //            }
        //            Thread.sleep(1000);
        //        }
        //    }
        //
        //    @Override
        //    public void cancel() {
        //        isRunning = false;
        //        try {
        //            currentSocket.close();
        //        } catch (Throwable t) {
        //            // ignore
        //        }
        //    }
        //}
        final DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

        // use logical types during runtime for parsing
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // create runtime class
        return new ChangelogCsvDeserializer(parsingTypes, converter, producedTypeInfo, columnDelimiter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // define that this format can produce INSERT and DELETE rows
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}