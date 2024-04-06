package io.github.shawn.octopus.fluxus.engine.connector.source.file;

import io.github.shawn.octopus.fluxus.api.config.Column;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.api.serialization.DeserializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.FileFormat;
import io.github.shawn.octopus.fluxus.engine.common.file.FileSystemType;
import io.github.shawn.octopus.fluxus.engine.common.file.HdfsConfig;
import io.github.shawn.octopus.fluxus.engine.model.type.DataWorkflowFieldTypeParse;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class FileSource implements Source<FileSourceConfig> {

  private final FileSourceConfig config;
  private final FileSourceConfig.FileSourceOptions options;
  private FileSourceConverter converter;
  private FileSystem fs;
  private DeserializationSchema<RowRecord> deserialization;
  private Iterator<String> lines;
  private BufferedReader reader;

  public FileSource(FileSourceConfig config) {
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public void init() throws StepExecutionException {
    log.info("file source {} init beginning...", config.getId());
    List<Column> columns = config.getColumns();
    String[] fieldNames = new String[columns.size()];
    DataWorkflowFieldType[] fieldTypes = new DataWorkflowFieldType[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      fieldNames[i] = columns.get(i).getName();
      fieldTypes[i] = DataWorkflowFieldTypeParse.parseDataType(columns.get(i).getType());
    }
    RowFieldType rowType = new RowFieldType(fieldNames, fieldTypes);
    FileFormat format = options.getFormat();
    deserialization = format.createDeserialization(rowType, config.getOptions());
    try {
      HdfsConfig hdfsConfig = config.getFileSystemConfig();
      FileSystemType type = hdfsConfig.getType();
      Configuration configuration =
          hdfsConfig.getConfiguration(type.getSchema(), type.getFileSystem());
      fs = FileSystem.get(configuration);
      reader =
          new BufferedReader(
              new InputStreamReader(
                  fs.open(new Path(config.getOptions().getPath())), StandardCharsets.UTF_8));
      lines = reader.lines().iterator();
      // 如果是csv，根据配置文件，第一行是否为header，如果为header，需要skip掉。
      if (format == FileFormat.CSV) {
        FileSourceConfig.CSVFileSourceOptions csvFileSourceOptions =
            (FileSourceConfig.CSVFileSourceOptions) options;
        boolean header = csvFileSourceOptions.isHeader();
        if (header) {
          lines.next();
        }
      }
      log.info("file source {} init success...", config.getId());
    } catch (Exception e) {
      log.error("file source {} init error...", config.getId(), e);
      throw new StepExecutionException(e);
    }
  }

  @Override
  public RowRecord read() throws StepExecutionException {
    try {
      if (lines.hasNext()) {
        String next = lines.next();
        return converter.convert(next.getBytes(StandardCharsets.UTF_8));
      }
      return null;
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public boolean commit() throws StepExecutionException {
    return true;
  }

  @Override
  public void abort() throws StepExecutionException {}

  @Override
  public <S> void setConverter(RowRecordConverter<S> converter) {
    this.converter = (FileSourceConverter) converter;
    this.converter.setDeserialization(deserialization);
  }

  @Override
  public FileSourceConfig getSourceConfig() {
    return config;
  }

  @Override
  public void dispose() throws StepExecutionException {
    log.info("file source {} close resources beginning...", config.getId());
    try {
      if (reader != null) {
        reader.close();
      }
      if (fs != null) {
        fs.close();
      }
      // for gc
      lines = null;
      log.info("file source {} close resources success...", config.getId());
    } catch (IOException e) {
      log.error("file source {} close resources error...", config.getId(), e);
      throw new StepExecutionException(e);
    }
  }
}
