package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.config.Column;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class PulsarSourceConfigTests {

  @Test
  public void testParseConfig() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/source/pulsar.json")),
            StandardCharsets.UTF_8);
    PulsarSourceConfig pulsarSourceConfig = new PulsarSourceConfig();
    PulsarSourceConfig sourceConfig = (PulsarSourceConfig) pulsarSourceConfig.toSourceConfig(json);
    assertNotNull(sourceConfig);
    assertEquals("pulsar", sourceConfig.getName());
    assertEquals("pulsar-output", sourceConfig.getOutput());

    PulsarSourceConfig.PulsarSourceOptions options = sourceConfig.getOptions();
    assertNotNull(options);
    assertEquals("batch-e2e", options.getTopic());

    List<Column> columns = sourceConfig.getColumns();
    assertNotNull(columns);
    assertEquals(3, columns.size());
    Column column = columns.get(0);
    assertEquals("id", column.getName());
    assertEquals("long", column.getType());
  }
}
