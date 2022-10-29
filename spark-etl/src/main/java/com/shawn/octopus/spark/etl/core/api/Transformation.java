package com.shawn.octopus.spark.etl.core.api;

import java.util.List;

public interface Transformation {

  String getId();

  List<Operation> getOperations();

  List<Hop> getHops();
}
