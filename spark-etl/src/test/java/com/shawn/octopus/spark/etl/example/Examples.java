package com.shawn.octopus.spark.etl.example;

import com.shawn.octopus.spark.etl.core.api.StepFactory;
import com.shawn.octopus.spark.etl.core.common.TableDesc;
import com.shawn.octopus.spark.etl.core.enums.SinkType;
import com.shawn.octopus.spark.etl.core.enums.SourceType;
import com.shawn.octopus.spark.etl.core.enums.TransformType;
import com.shawn.octopus.spark.etl.core.factory.DefaultStepFactory;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import com.shawn.octopus.spark.etl.sink.Sink;
import com.shawn.octopus.spark.etl.sink.file.csv.CSVSinkOptions;
import com.shawn.octopus.spark.etl.source.Source;
import com.shawn.octopus.spark.etl.source.file.csv.CSVSourceOptions;
import com.shawn.octopus.spark.etl.transform.Transform;
import com.shawn.octopus.spark.etl.transform.sparksql.SparkSQLTransformOptions;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class Examples {

  @Test
  public void testSimple() {
    String path = Examples.class.getClassLoader().getResource("user_visit_action.csv").getPath();
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
      Transform transform =
          stepFactory.createTransform(
              TransformType.SPARK_SQL,
              "test-transform",
              SparkSQLTransformOptions.builder()
                  .sql("select * from t1 where _c12 > 13")
                  .output("t2")
                  .build());

      Sink sink =
          stepFactory.createSink(
              SinkType.csv,
              "test-sink",
              CSVSinkOptions.builder()
                  .filePath(Examples.class.getClassLoader().getResource("").getPath() + "output")
                  .input("t2")
                  .header(false)
                  .build());

      // 执行source
      source.process(context);
      // 执行transform
      transform.process(context);
      // 执行sink
      sink.process(context);
    }
  }
}
