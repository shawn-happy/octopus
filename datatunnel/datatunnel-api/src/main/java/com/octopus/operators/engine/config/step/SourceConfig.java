package com.octopus.operators.engine.config.step;

import com.octopus.operators.engine.table.catalog.Column;
import java.util.List;

public interface SourceConfig<P extends StepOptions> extends StepConfig<P> {

  @Override
  default PluginType getPluginType() {
    return PluginType.SOURCE;
  }

  String getResultTable();

  List<Column> getColumns();
}
