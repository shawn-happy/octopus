package com.octopus.operators.connector.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class CSVSink {

  //  private final FlinkRuntimeEnvironment environment;
  //  private final CSVSinkDeclare declare;
  //
  //  public CSVSink(FlinkRuntimeEnvironment environment, CSVSinkDeclare declare) {
  //    this.environment = environment;
  //    this.declare = declare;
  //  }

  //  @Override
  public void writer(DataStream<Row> dataStream) throws Exception {
    //    StreamTableEnvironment tableEnvironment = environment.getStreamTableEnvironment();
    //    ResolvedSchema resolvedSchema =
    // tableEnvironment.fromDataStream(dataStream).getResolvedSchema();
    //    List<RowType.RowField> fields =
    //        resolvedSchema.getColumns().stream()
    //            .map(column -> new RowType.RowField(column.getName(), new VarCharType()))
    //            .collect(Collectors.toUnmodifiableList());
    //    var fileSink =
    //        FileSink.forRowFormat(
    //                new Path(declare.getOptions().getPath()),
    //                new SerializationSchemaAdapter(
    //                    new CsvRowDataSerializationSchema.Builder(new RowType(fields)).build()))
    //            .withRollingPolicy(
    //                DefaultRollingPolicy.builder()
    //                    .withRolloverInterval(Duration.ofMinutes(15))
    //                    .withInactivityInterval(Duration.ofMinutes(5))
    //                    .withMaxPartSize(MemorySize.ofMebiBytes(1024))
    //                    .build())
    //            .build();
  }
}
