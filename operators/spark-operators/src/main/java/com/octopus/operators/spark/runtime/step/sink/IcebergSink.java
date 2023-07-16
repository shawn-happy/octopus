package com.octopus.operators.spark.runtime.step.sink;

import com.google.common.collect.Maps;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.IcebergSinkDeclare;
import com.octopus.operators.spark.declare.sink.IcebergSinkDeclare.IcebergSinkOptions;
import com.octopus.operators.spark.utils.SparkOperatorUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.CreateTableWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class IcebergSink extends BaseSink<IcebergSinkDeclare> {

  public IcebergSink(IcebergSinkDeclare declare) {
    super(declare);
  }

  @Override
  protected void process(SparkSession spark, Dataset<Row> df) throws Exception {
    IcebergSinkOptions options = declare.getOptions();
    String icebergTableName = options.getFullTableName();
    WriteMode writeMode = declare.getWriteMode();
    if (writeMode != WriteMode.create_or_replace) {
      // schema check
      StructType sourceSchema = df.schema();
      StructType targetSchema = spark.read().table(icebergTableName).schema();
      Column[] columns = SparkOperatorUtils.validateSchema(sourceSchema, targetSchema);
      if (columns != null) {
        df = df.select(columns);
      } else {
        spark.close();
        System.exit(1);
      }
    }

    if (options.getPartitionBy() != null && !options.getPartitionBy().isEmpty()) {
      df =
          df.sortWithinPartitions(
              options.getPartitionBy().stream().map(functions::col).toArray(Column[]::new));
    }

    switch (writeMode) {
      case append:
        df.writeTo(icebergTableName).append();
        break;
      case overwrite_partitions:
        df.writeTo(icebergTableName).overwritePartitions();
        break;
      case create_or_replace:
        CreateTableWriter<Row> writer = df.writeTo(icebergTableName);
        if (options.getTableProperties() != null) {
          for (Map.Entry<String, String> entry :
              Maps.fromProperties(options.getTableProperties()).entrySet()) {
            writer = writer.tableProperty(entry.getKey(), entry.getValue());
          }
        }
        writer.createOrReplace();
        break;
      case replace:
        df.createTempView("tmp_source");
        String sql =
            SparkOperatorUtils.createRTASSQL(
                icebergTableName,
                options.getPartitionExpressions(),
                options.getTableProperties(),
                "SELECT * FROM tmp_source");
        log.info("RTAS SQL: {}", sql);
        spark.sql(sql);
        break;
      case replace_by_time:
        spark.sql(
            "DELETE FROM "
                + icebergTableName
                + " WHERE "
                + options.getDateField()
                + ">='"
                + options.getReplaceRangeStart()
                + "' AND "
                + options.getDateField()
                + "<'"
                + options.getReplaceRangeEnd()
                + "'");
        df.writeTo(icebergTableName).append();
        break;
      default:
        throw new IllegalArgumentException("unsupported write mode: " + writeMode);
    }
  }
}
