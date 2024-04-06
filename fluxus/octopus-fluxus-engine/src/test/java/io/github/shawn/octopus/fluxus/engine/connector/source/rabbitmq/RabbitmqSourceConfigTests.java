package io.github.shawn.octopus.fluxus.engine.connector.source.rabbitmq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.config.Column;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class RabbitmqSourceConfigTests {

  @Test
  public void testParseConfig() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/source/rabbitmq.json")),
            StandardCharsets.UTF_8);
    RabbitmqSourceConfig rabbitmqSourceConfig = new RabbitmqSourceConfig();
    RabbitmqSourceConfig sourceConfig =
        (RabbitmqSourceConfig) rabbitmqSourceConfig.toSourceConfig(json);
    assertNotNull(sourceConfig);
    assertEquals("rabbitmq", sourceConfig.getName());
    assertEquals("rabbitmq-output", sourceConfig.getOutput());

    RabbitmqSourceConfig.RabbitmqSourceOptions options = sourceConfig.getOptions();
    assertNotNull(options);
    assertEquals("batch-e2e", options.getQueueName());

    List<Column> columns = sourceConfig.getColumns();
    assertNotNull(columns);
    assertEquals(3, columns.size());
    Column column = columns.get(0);
    assertEquals("id", column.getName());
    assertEquals("long", column.getType());
  }
}
