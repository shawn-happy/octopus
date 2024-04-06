package io.github.shawn.octopus.fluxus.engine.common.file.serialization;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.api.serialization.DeserializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.format.JacksonFormatters;
import io.github.shawn.octopus.fluxus.engine.common.file.format.JacksonToObjectFormatter;
import io.github.shawn.octopus.fluxus.engine.connector.source.file.FileSourceConfig;
import io.github.shawn.octopus.fluxus.engine.model.type.ArrayFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;

public class CSVToRowRecordDeserialization implements DeserializationSchema<RowRecord> {

  private final JacksonToObjectFormatter jacksonFormatter;

  private final CsvMapper csvMapper =
      CsvMapper.builder()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true)
          .findAndAddModules()
          .build();
  private final CsvSchema csvSchema;

  public CSVToRowRecordDeserialization(
      RowFieldType rowFieldType, FileSourceConfig.CSVFileSourceOptions options) {
    CsvSchema.Builder builder = CsvSchema.builder();
    builder.setColumnSeparator(options.getSeparator());
    builder.setArrayElementSeparator(options.getArrayElementSeparator());
    String[] fieldNames = rowFieldType.getFieldNames();
    DataWorkflowFieldType[] fieldTypes = rowFieldType.getFieldTypes();
    for (int i = 0; i < fieldNames.length; i++) {
      String col = fieldNames[i];
      DataWorkflowFieldType fieldType = fieldTypes[i];
      if (fieldType instanceof ArrayFieldType) {
        builder.addArrayColumn(col);
      } else {
        builder.addColumn(col);
      }
    }
    csvSchema = builder.build();
    jacksonFormatter = JacksonFormatters.createJacksonToObjectFormatter(rowFieldType);
  }

  @Override
  public RowRecord deserialize(byte[] message) throws IOException {
    if (ArrayUtils.isEmpty(message)) {
      return null;
    }
    return convertJsonNode(convertBytes(message));
  }

  private RowRecord convertJsonNode(JsonNode jsonNode) {
    if (jsonNode.isNull()) {
      return null;
    }
    try {
      return (RowRecord) jacksonFormatter.format(jsonNode);
    } catch (Throwable t) {
      throw new DataWorkflowException(
          String.format("Failed to deserialize JSON '%s'.", jsonNode), t);
    }
  }

  private JsonNode convertBytes(byte[] message) {
    try {
      return csvMapper.readerFor(String.class).with(csvSchema).readTree(message);
    } catch (Throwable t) {
      throw new DataWorkflowException(
          String.format("Failed to deserialize JSON '%s'.", new String(message)), t);
    }
  }
}
