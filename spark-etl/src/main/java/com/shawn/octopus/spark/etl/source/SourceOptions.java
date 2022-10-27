package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.common.TableDesc;
import com.shawn.octopus.spark.etl.core.step.StepConfiguration;
import java.util.List;

public interface SourceOptions extends StepConfiguration {

  /** Source的输入为空 */
  @Override
  default List<TableDesc> getInputs() {
    return null;
  }
}
