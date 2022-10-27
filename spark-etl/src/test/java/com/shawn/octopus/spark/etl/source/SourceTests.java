package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.common.TableDesc;
import com.shawn.octopus.spark.etl.core.enums.Format;
import com.shawn.octopus.spark.etl.core.factory.DefaultStepFactory;
import com.shawn.octopus.spark.etl.core.factory.StepFactory;
import com.shawn.octopus.spark.etl.core.step.StepContext;
import com.shawn.octopus.spark.etl.source.file.csv.CSVSourceOptions;
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
      TableDesc output = new TableDesc();
      output.setAlias("t1");
      StepFactory stepFactory = DefaultStepFactory.getStepFactory();

      Source source =
          stepFactory.createSource(
              "test-csv",
              Format.csv,
              CSVSourceOptions.builder()
                  .paths(new String[] {path})
                  .header(false)
                  .outputs(List.of(output))
                  .build());
      source.process(context);
      Dataset<Row> df = context.getDataFrame("t1");
      df.show(10);
    }
  }
}
