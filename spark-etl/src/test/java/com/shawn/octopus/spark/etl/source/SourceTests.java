package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.Output;
import com.shawn.octopus.spark.etl.core.StepContext;
import com.shawn.octopus.spark.etl.source.file.CSVSource;
import com.shawn.octopus.spark.etl.source.file.CSVSourceConfig;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class SourceTests {

  @Test
  public void testReadCSV() throws Exception {
    String path = SourceTests.class.getClassLoader().getResource("user_visit_action.csv").getPath();
    try (SparkSession spark =
        SparkSession.builder().appName("spark-etl").master("local[*]").getOrCreate()) {
      StepContext context = new StepContext(spark);
      CSVSourceConfig csvSourceConfig =
          CSVSourceConfig.builder().paths(new String[] {path}).header(false).build();
      Output output = new Output();
      output.setAlias("t1");
      csvSourceConfig.setOutputs(List.of(output));
      CSVSource source = new CSVSource(context, csvSourceConfig);
      source.processRow();
      Dataset<Row> df = context.getDataFrame("t1");
      df.show(10);
    }
  }
}
