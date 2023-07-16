package com.octopus.spark.operators.runtime.step.source;

import com.octopus.spark.operators.declare.source.SourceDeclare;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Source<SD extends SourceDeclare<?>> {

  Dataset<Row> input(SparkSession spark) throws Exception;
}
