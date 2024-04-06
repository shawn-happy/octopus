package io.github.shawn.octopus.fluxus.engine.common.file.serialization;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.api.serialization.SerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.format.JacksonFormatters;
import io.github.shawn.octopus.fluxus.engine.common.file.format.ObjectToJacksonFormatter;
import io.github.shawn.octopus.fluxus.engine.connector.sink.file.FileSinkConfig;
import io.github.shawn.octopus.fluxus.engine.model.type.ArrayFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;

public class CSVSerializationSchema implements SerializationSchema {

  private final CsvMapper csvMapper =
      CsvMapper.builder()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true)
          .findAndAddModules()
          .build();
  private final CsvSchema csvSchema;
  private ObjectNode node;
  private final ObjectToJacksonFormatter jacksonFormatter;

  public CSVSerializationSchema(
      RowFieldType rowFieldType, FileSinkConfig.CSVFileSinkOptions options) {
    this(rowFieldType, options.getSeparator(), options.getArrayElementSeparator());
  }

  public CSVSerializationSchema(
      RowFieldType rowFieldType, char separator, String arrayElementSeparator) {
    CsvSchema.Builder builder = CsvSchema.builder();
    builder.setColumnSeparator(separator);
    builder.setArrayElementSeparator(arrayElementSeparator);
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
    this.jacksonFormatter = JacksonFormatters.createObjectToJacksonFormatter(rowFieldType);
  }

  @Override
  public byte[] serialize(RowRecord record) {
    if (node == null) {
      node = csvMapper.createObjectNode();
    }

    try {
      jacksonFormatter.format(csvMapper, node, record);
      return csvMapper.writer(csvSchema).writeValueAsBytes(node);
    } catch (Throwable e) {
      throw new DataWorkflowException(String.format("Failed to deserialize CSV '%s'.", record), e);
    }
  }
}
