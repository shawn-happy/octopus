package com.octopus.operators.flink.runtime.sink;

import com.octopus.operators.flink.declare.sink.CSVSinkDeclare;
import com.octopus.operators.flink.runtime.FlinkRuntimeEnvironment;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.table.SerializationSchemaAdapter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvRowDataSerializationSchema;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class CSVSink implements Sink<CSVSinkDeclare> {

  private final FlinkRuntimeEnvironment environment;
  private final CSVSinkDeclare declare;

  public CSVSink(FlinkRuntimeEnvironment environment, CSVSinkDeclare declare) {
    this.environment = environment;
    this.declare = declare;
  }

  @Override
  public void writer(DataStream<Row> dataStream) throws Exception {
    StreamTableEnvironment tableEnvironment = environment.getStreamTableEnvironment();
    ResolvedSchema resolvedSchema = tableEnvironment.fromDataStream(dataStream).getResolvedSchema();
    List<RowType.RowField> fields =
        resolvedSchema.getColumns().stream()
            .map(column -> new RowType.RowField(column.getName(), new VarCharType()))
            .collect(Collectors.toUnmodifiableList());
    var fileSink =
        FileSink.forRowFormat(
                new Path(declare.getOptions().getPath()),
                new SerializationSchemaAdapter(
                    new CsvRowDataSerializationSchema.Builder(new RowType(fields)).build()))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(15))
                    .withInactivityInterval(Duration.ofMinutes(5))
                    .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                    .build())
            .build();


  }
}
