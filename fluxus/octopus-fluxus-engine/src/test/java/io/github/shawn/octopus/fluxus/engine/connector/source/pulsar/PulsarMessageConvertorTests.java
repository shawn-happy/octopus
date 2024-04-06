package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import io.github.shawn.octopus.fluxus.engine.model.table.Schema;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PulsarMessageConvertorTests {

  @Test
  public void test() {
    String json = "{\n" + "  \"id\": 1,\n" + "  \"name\": \"shawn\",\n" + "  \"age\": 26\n" + "}";
    List<Schema> schemas = new ArrayList<>();
    schemas.add(Schema.builder().fieldName("id").fieldType(BasicFieldType.BIGINT).build());
    schemas.add(Schema.builder().fieldName("name").fieldType(BasicFieldType.STRING).build());
    schemas.add(Schema.builder().fieldName("age").fieldType(BasicFieldType.INT).build());
    PulsarMessageConverter rowRecordConvertor = new PulsarMessageConverter(schemas);
    Message<byte[]> message = Mockito.mock(Message.class);
    Mockito.when(message.getValue()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
    SourceRowRecord record = rowRecordConvertor.convert(message);
    Object[] values = record.pollNext();
    Assertions.assertNotNull(record);
    Assertions.assertEquals(1, values[0]);
    Assertions.assertEquals("shawn", values[1]);
    Assertions.assertEquals(26, values[2]);
  }
}
