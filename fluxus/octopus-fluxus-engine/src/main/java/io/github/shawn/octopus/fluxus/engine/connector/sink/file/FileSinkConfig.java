package io.github.shawn.octopus.fluxus.engine.connector.sink.file;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseSinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.common.file.FileFormat;
import io.github.shawn.octopus.fluxus.engine.common.file.HdfsConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileSinkConfig extends BaseSinkConfig<FileSinkConfig.FileSinkOptions>
    implements SinkConfig<FileSinkConfig.FileSinkOptions> {

  private String id;
  private String name;
  @Builder.Default private String identifier = Constants.SinkConstants.FILE;
  private String input;
  private FileSinkOptions options;
  private HdfsConfig fileSystemConfig;

  @Override
  protected void checkOptions() {}

  @Override
  protected void loadSinkConfig(String json) {
    FileSinkConfig fileSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<FileSinkConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("file sink config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(fileSinkConfig.getId())
            ? fileSinkConfig.getId()
            : IdGenerator.uuid();
    this.name = fileSinkConfig.getName();
    this.input = fileSinkConfig.getInput();
    this.options = fileSinkConfig.getOptions();
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = FileSinkConfig.CSVFileSinkOptions.class, name = "CSV"),
    @JsonSubTypes.Type(value = FileSinkConfig.JsonFileSinkOptions.class, name = "JSON"),
    @JsonSubTypes.Type(value = FileSinkConfig.ParquetFileSinkOptions.class, name = "PARQUET"),
    @JsonSubTypes.Type(value = FileSinkConfig.OrcFileSinkOptions.class, name = "ORC")
  })
  public static class FileSinkOptions implements SinkConfig.SinkOptions {
    private String path;
    private String[] sinkColumns;

    public FileFormat getFormat() {
      return null;
    }
  }

  @Getter
  @Setter
  @JsonTypeName("CSV")
  public static class CSVFileSinkOptions extends FileSinkOptions {
    private final FileFormat format = FileFormat.CSV;
    private char separator = ',';
    private String arrayElementSeparator = ";";
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeName("JSON")
  public static class JsonFileSinkOptions extends FileSinkOptions {
    private final FileFormat format = FileFormat.JSON;
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeName("PARQUET")
  public static class ParquetFileSinkOptions extends FileSinkOptions {
    private final FileFormat format = FileFormat.PARQUET;
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeName("ORC")
  public static class OrcFileSinkOptions extends FileSinkOptions {
    private final FileFormat format = FileFormat.ORC;
  }
}
