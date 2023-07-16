package com.octopus.operators.spark.runtime.step.source;

import com.octopus.operators.spark.declare.source.SourceDeclare;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Source<SD extends SourceDeclare<?>> {

  Dataset<Row> input(SparkSession spark) throws Exception;
}
