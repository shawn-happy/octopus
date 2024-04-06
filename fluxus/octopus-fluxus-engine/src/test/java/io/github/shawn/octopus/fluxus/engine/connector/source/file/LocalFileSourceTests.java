package io.github.shawn.octopus.fluxus.engine.connector.source.file;

import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.config.Column;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.api.serialization.DeserializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.FileFormat;
import io.github.shawn.octopus.fluxus.engine.model.type.DataWorkflowFieldTypeParse;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class LocalFileSourceTests {
  @Test
  public void testLocalFileSourceJson() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/source/local-file.json")),
            StandardCharsets.UTF_8);
    FileSourceConfig sourceConfig = new FileSourceConfig();
    FileSourceConfig fileSourceConfig = (FileSourceConfig) sourceConfig.toSourceConfig(json);
    FileSource fileSource = new FileSource(fileSourceConfig);
    List<Column> columns = fileSourceConfig.getColumns();
    String[] fieldNames = new String[columns.size()];
    DataWorkflowFieldType[] fieldTypes = new DataWorkflowFieldType[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      fieldNames[i] = columns.get(i).getName();
      fieldTypes[i] = DataWorkflowFieldTypeParse.parseDataType(columns.get(i).getType());
    }
    RowFieldType rowType = new RowFieldType(fieldNames, fieldTypes);
    FileFormat format = fileSourceConfig.getOptions().getFormat();
    DeserializationSchema<RowRecord> deserialization =
        format.createDeserialization(rowType, fileSourceConfig.getOptions());
    fileSource.init();
    FileSourceConverter fileSourceConverter = new FileSourceConverter();
    fileSourceConverter.setDeserialization(deserialization);
    fileSource.setConverter(fileSourceConverter);
    RowRecord record;
    while ((record = fileSource.read()) != null) {
      System.out.println(Arrays.toString(record.pollNext()));
    }
  }

  @Test
  public void testLocalFileSourceCsv() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/source/local-file-csv.json")),
            StandardCharsets.UTF_8);
    FileSourceConfig sourceConfig = new FileSourceConfig();
    FileSourceConfig fileSourceConfig = (FileSourceConfig) sourceConfig.toSourceConfig(json);
    FileSource fileSource = new FileSource(fileSourceConfig);
    List<Column> columns = fileSourceConfig.getColumns();
    String[] fieldNames = new String[columns.size()];
    DataWorkflowFieldType[] fieldTypes = new DataWorkflowFieldType[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      fieldNames[i] = columns.get(i).getName();
      fieldTypes[i] = DataWorkflowFieldTypeParse.parseDataType(columns.get(i).getType());
    }
    RowFieldType rowType = new RowFieldType(fieldNames, fieldTypes);
    FileFormat format = fileSourceConfig.getOptions().getFormat();
    DeserializationSchema<RowRecord> deserialization =
        format.createDeserialization(rowType, fileSourceConfig.getOptions());
    fileSource.init();
    FileSourceConverter fileSourceConverter = new FileSourceConverter();
    fileSourceConverter.setDeserialization(deserialization);
    fileSource.setConverter(fileSourceConverter);
    RowRecord record;
    while ((record = fileSource.read()) != null) {
      System.out.println(Arrays.toString(record.pollNext()));
    }
  }
}
