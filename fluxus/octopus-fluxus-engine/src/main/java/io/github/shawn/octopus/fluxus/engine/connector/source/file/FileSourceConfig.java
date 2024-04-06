package io.github.shawn.octopus.fluxus.engine.connector.source.file;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseSourceConfig;
import io.github.shawn.octopus.fluxus.api.config.Column;
import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.common.file.FileFormat;
import io.github.shawn.octopus.fluxus.engine.common.file.HdfsConfig;
import java.util.List;
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
public class FileSourceConfig extends BaseSourceConfig<FileSourceConfig.FileSourceOptions>
    implements SourceConfig<FileSourceConfig.FileSourceOptions> {

  private String id;
  private String name;
  private FileSourceOptions options;
  private String output;
  private List<Column> columns;
  private HdfsConfig fileSystemConfig;

  @Override
  protected void checkOptions() {
    options.check();
    String hdfsNameKey = fileSystemConfig.getHdfsNameKey();
    verify(StringUtils.isNotBlank(hdfsNameKey), "hdfs namenode path cannot be null");
  }

  @Override
  protected void loadSourceConfig(String json) {
    FileSourceConfig sourceConfig =
        JsonUtils.fromJson(json, new TypeReference<FileSourceConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("activemq config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(sourceConfig.getId()) ? sourceConfig.getId() : IdGenerator.uuid();
    this.name = sourceConfig.getName();
    this.output = sourceConfig.getOutput();
    this.options = sourceConfig.getOptions();
    this.columns = sourceConfig.getColumns();
    this.fileSystemConfig = sourceConfig.getFileSystemConfig();
  }

  @Override
  public String getIdentifier() {
    return Constants.SourceConstants.FILE;
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = CSVFileSourceOptions.class, name = "CSV"),
    @JsonSubTypes.Type(value = JsonFileSourceOptions.class, name = "JSON"),
    @JsonSubTypes.Type(value = ParquetFileSourceOptions.class, name = "PARQUET"),
    @JsonSubTypes.Type(value = OrcFileSourceOptions.class, name = "ORC")
  })
  public static class FileSourceOptions implements SourceConfig.SourceOptions {
    private String path;
    private String dateFormat = "yyyy-MM-dd";
    private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    protected void check() {}

    public FileFormat getFormat() {
      return null;
    }
  }

  @Getter
  @Setter
  @JsonTypeName("CSV")
  public static class CSVFileSourceOptions extends FileSourceOptions {

    private final FileFormat format = FileFormat.CSV;
    private boolean header = true;
    private char separator = ',';
    private String arrayElementSeparator = ";";
    private String nullValue;

    @Override
    protected void check() {}
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeName("JSON")
  public static class JsonFileSourceOptions extends FileSourceOptions {
    private final FileFormat format = FileFormat.JSON;

    @Override
    protected void check() {}
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeName("PARQUET")
  public static class ParquetFileSourceOptions extends FileSourceOptions {
    private final FileFormat format = FileFormat.PARQUET;

    @Override
    protected void check() {}
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonTypeName("ORC")
  public static class OrcFileSourceOptions extends FileSourceOptions {
    private final FileFormat format = FileFormat.ORC;

    @Override
    protected void check() {}
  }
}
