package io.github.shawn.octopus.data.fluxus.engine.connector.source.rabbitmq;

import io.github.shawn.octopus.data.fluxus.engine.model.table.Schema;
import io.github.shawn.octopus.data.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.data.fluxus.engine.model.type.BasicFieldType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RabbitmqMessageConvertorTests {

  @Test
  public void test() {
    String json = "{\n" + "  \"id\": 1,\n" + "  \"name\": \"shawn\",\n" + "  \"age\": 26\n" + "}";
    List<Schema> schemas = new ArrayList<>();
    schemas.add(Schema.builder().fieldName("id").fieldType(BasicFieldType.BIGINT).build());
    schemas.add(Schema.builder().fieldName("name").fieldType(BasicFieldType.STRING).build());
    schemas.add(Schema.builder().fieldName("age").fieldType(BasicFieldType.INT).build());
    RabbitmqMessageConverter rowRecordConvertor = new RabbitmqMessageConverter(schemas);
    String message = json;
    SourceRowRecord record = rowRecordConvertor.convert(message.getBytes());
    Object[] values = record.pollNext();
    Assertions.assertNotNull(record);
    Assertions.assertEquals(1, values[0]);
    Assertions.assertEquals("shawn", values[1]);
    Assertions.assertEquals(26, values[2]);
  }
}
