package com.octopus.operators.connector.spark.transform;

import com.octopus.operators.connector.spark.DatasetTableInfo;
import com.octopus.operators.connector.spark.SparkAbstractExecuteProcessor;
import com.octopus.operators.connector.spark.SparkJobContext;
import com.octopus.operators.connector.spark.SparkRuntimeEnvironment;
import com.octopus.operators.engine.connector.transform.Transform;
import com.octopus.operators.engine.connector.transform.sql.SqlTransformConfig;
import com.octopus.operators.engine.connector.transform.sql.SqlTransformConfig.SqlTransformOptions;
import com.octopus.operators.engine.exception.EngineException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlTransform extends SparkAbstractExecuteProcessor
    implements Transform<DatasetTableInfo, SparkRuntimeEnvironment> {

  private final SqlTransformConfig config;
  private final SqlTransformOptions options;

  public SqlTransform(
      SparkRuntimeEnvironment sparkRuntimeEnvironment,
      SparkJobContext jobContext,
      SqlTransformConfig config) {
    super(sparkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public DatasetTableInfo transform() {
    SparkSession sparkSession = sparkRuntimeEnvironment.getSparkSession();
    List<String> inputs = config.getInputs();
    boolean exists = jobContext.tableExists(inputs);
    if (!exists) {
      throw new EngineException("table not found");
    }
    Dataset<Row> sql = sparkSession.sql(options.getSql());
    jobContext.registerTable(config.getOutput(), sql);
    return DatasetTableInfo.of(config.getOutput(), sql);
  }
}
