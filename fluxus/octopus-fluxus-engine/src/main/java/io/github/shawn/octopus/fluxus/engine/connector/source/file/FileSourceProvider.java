package io.github.shawn.octopus.fluxus.engine.connector.source.file;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SourceConstants.FILE;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.provider.SourceProvider;

public class FileSourceProvider implements SourceProvider<FileSourceConfig, byte[]> {

  @Override
  public String getIdentifier() {
    return FILE;
  }

  @Override
  public FileSourceConfig getSourceConfig() {
    return new FileSourceConfig();
  }

  @Override
  public Source<FileSourceConfig> getSource(SourceConfig<?> sourceConfig) {
    return new FileSource((FileSourceConfig) sourceConfig);
  }

  @Override
  public RowRecordConverter<byte[]> getConvertor(SourceConfig<?> sourceConfig) {
    return new FileSourceConverter();
  }
}
