package io.github.shawn.octopus.fluxus.engine.connector.sink.file;

import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.api.serialization.SerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.FileFormat;
import io.github.shawn.octopus.fluxus.engine.common.file.FileSystemType;
import io.github.shawn.octopus.fluxus.engine.common.file.HdfsConfig;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class FileSink implements Sink<FileSinkConfig> {

  private static final int WRITE_BUFFER_SIZE = 2048;

  private final FileSinkConfig config;
  private final FileSinkConfig.FileSinkOptions options;
  private FileSystem fs;
  private FSDataOutputStream fsDataOutputStream;
  private SerializationSchema serializationSchema;
  private boolean first;

  public FileSink(FileSinkConfig config) {
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public void init() throws StepExecutionException {
    log.info("file sink {} init beginning", config.getId());
    HdfsConfig hdfsConfig = config.getFileSystemConfig();
    FileSystemType type = hdfsConfig.getType();
    Configuration configuration =
        hdfsConfig.getConfiguration(type.getSchema(), type.getFileSystem());
    try {
      fs = FileSystem.get(configuration);
      fsDataOutputStream = fs.create(new Path(options.getPath()), true, WRITE_BUFFER_SIZE);
      log.info("file sink {} init success", config.getId());
    } catch (IOException e) {
      log.error("file sink {} init error", config.getId(), e);
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void write(RowRecord source) throws StepExecutionException {
    if (source == null) {
      return;
    }
    if (serializationSchema == null) {
      FileFormat format = options.getFormat();
      String[] sinkColumns = options.getSinkColumns();
      DataWorkflowFieldType[] sinkFieldTypes;
      if (ArrayUtils.isEmpty(sinkColumns)) {
        sinkColumns = source.getFieldNames();
        sinkFieldTypes = source.getFieldTypes();
      } else {
        sinkFieldTypes = new DataWorkflowFieldType[sinkColumns.length];
        for (int i = 0; i < sinkColumns.length; i++) {
          String sinkColumn = sinkColumns[i];
          int idx = source.indexOf(sinkColumn);
          if (idx < 0) {
            throw new StepExecutionException(
                String.format(
                    "cannot find this column [%s] in row-record [%s]",
                    sinkColumn, Arrays.toString(source.getFieldNames())));
          }
          sinkFieldTypes[i] = source.getFieldType(idx);
        }
      }
      RowFieldType rowFieldType = new RowFieldType(sinkColumns, sinkFieldTypes);
      serializationSchema = format.createSerialization(rowFieldType, options);
      first = true;
    }
    byte[] serialize = serializationSchema.serialize(source);
    try {
      if (!first && options.getFormat() == FileFormat.JSON) {
        fsDataOutputStream.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
      }
      fsDataOutputStream.write(serialize);
      first = false;
    } catch (IOException e) {
      log.error("file sink {} write {} error", config.getId(), source.getValues(), e);
      throw new StepExecutionException(e);
    }
  }

  @Override
  public FileSinkConfig getSinkConfig() {
    return config;
  }

  @Override
  public boolean commit() throws StepExecutionException {
    return true;
  }

  @Override
  public void abort() throws StepExecutionException {}

  @Override
  public void dispose() throws StepExecutionException {
    log.info("file sink {} close resources beginning...", config.getId());
    try {
      if (fsDataOutputStream != null) {
        fsDataOutputStream.close();
      }
      if (fs != null) {
        fs.close();
      }
      // for gc
      serializationSchema = null;
      log.info("file sink {} close resources success...", config.getId());
    } catch (IOException e) {
      log.error("file sink {} close resources error", config.getId(), e);
      throw new StepExecutionException(e);
    }
  }
}
