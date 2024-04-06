package io.github.shawn.octopus.fluxus.engine.common.file.serialization;

import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.api.serialization.SerializationSchema;
import io.github.shawn.octopus.fluxus.engine.connector.sink.file.FileSinkConfig;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RowRecordToJsonSerializationTests {

  @Test
  public void testRecordToJson() {
    final String[] fieldNames = {"id", "name"};
    final DataWorkflowFieldType[] fieldTypes = {BasicFieldType.INT, BasicFieldType.STRING};
    RowFieldType rowFieldType = new RowFieldType(fieldNames, fieldTypes);
    SerializationSchema serializationSchema = new JsonSerializationSchema(rowFieldType);
    RowRecord record =
        SourceRowRecord.builder()
            .fieldNames(fieldNames)
            .fieldTypes(fieldTypes)
            .values(new Object[] {1, "shawn"})
            .build();
    byte[] serialize = serializationSchema.serialize(record);
    Assertions.assertNotNull(serialize);
  }

  @Test
  public void testRecordToCSV() {
    final String[] fieldNames = {"id", "name"};
    final DataWorkflowFieldType[] fieldTypes = {BasicFieldType.INT, BasicFieldType.STRING};
    RowFieldType rowFieldType = new RowFieldType(fieldNames, fieldTypes);
    FileSinkConfig.CSVFileSinkOptions csvFileSinkOptions = new FileSinkConfig.CSVFileSinkOptions();
    SerializationSchema serializationSchema =
        new CSVSerializationSchema(rowFieldType, csvFileSinkOptions);
    RowRecord record =
        SourceRowRecord.builder()
            .fieldNames(fieldNames)
            .fieldTypes(fieldTypes)
            .values(new Object[] {1, "shawn"})
            .build();
    byte[] serialize = serializationSchema.serialize(record);
    Assertions.assertNotNull(serialize);
  }
}
