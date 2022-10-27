package com.shawn.octopus.spark.etl.core.step;

import com.shawn.octopus.spark.etl.core.common.TableDesc;
import java.util.List;
import java.util.Map;

public interface StepConfiguration {

  int getRePartitions();

  List<TableDesc> getInputs();

  List<TableDesc> getOutputs();

  Map<String, String> getOptions();
}
