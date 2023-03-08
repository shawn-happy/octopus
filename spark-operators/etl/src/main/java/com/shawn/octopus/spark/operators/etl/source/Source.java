package com.shawn.octopus.spark.operators.etl.source;

import com.shawn.octopus.spark.operators.common.declare.source.SourceDeclare;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Source<SD extends SourceDeclare<?>> {

  SD getDeclare();

  Dataset<Row> input(SparkSession spark) throws Exception;
}
