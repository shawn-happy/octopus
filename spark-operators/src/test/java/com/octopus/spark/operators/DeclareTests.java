package com.octopus.spark.operators;

import com.octopus.spark.operators.declare.ETLDeclare;
import com.octopus.spark.operators.utils.SparkOperatorUtils;
import org.junit.jupiter.api.Test;

public class DeclareTests {

  @Test
  public void createETLDeclare() {
    String path =
        DeclareTests.class.getClassLoader().getResource("etl-declare-example.yaml").getPath();
    ETLDeclare config = SparkOperatorUtils.getConfig(path, ETLDeclare.class);
    System.out.println(config);
  }
}
