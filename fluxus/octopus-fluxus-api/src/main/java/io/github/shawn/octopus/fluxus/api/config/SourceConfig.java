package io.github.shawn.octopus.fluxus.api.config;

import java.util.List;

public interface SourceConfig<P extends SourceConfig.SourceOptions> extends StepConfig {

  String getOutput();

  P getOptions();

  List<Column> getColumns();

  SourceConfig<P> toSourceConfig(String json);

  @Override
  default PluginType getPluginType() {
    return PluginType.SOURCE;
  }

  interface SourceOptions {}
}
