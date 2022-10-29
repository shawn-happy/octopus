package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.api.StepFactory;
import com.shawn.octopus.spark.etl.core.common.TableDesc;
import com.shawn.octopus.spark.etl.core.enums.SourceType;
import com.shawn.octopus.spark.etl.core.factory.DefaultStepFactory;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import com.shawn.octopus.spark.etl.source.file.csv.CSVSourceOptions;
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
      ETLContext context = new ETLContext(spark);
      TableDesc output = new TableDesc();
      output.setAlias("t1");
      StepFactory stepFactory = DefaultStepFactory.getStepFactory();

      Source source =
          stepFactory.createSource(
              SourceType.csv,
              "test-csv",
              CSVSourceOptions.builder()
                  .paths(new String[] {path})
                  .header(false)
                  .output("t1")
                  .build());
      source.process(context);
      Dataset<Row> df = context.getDataFrame("t1");
      df.show(10);
    }
  }
}
