package io.github.shawn.octopus.fluxus.engine.connector.source.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.config.Column;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class JdbcSourceTests {

  @Test
  public void testJdbcSourceConfig() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/source/jdbc.json")),
            StandardCharsets.UTF_8);
    JdbcSourceConfig jdbcSourceConfig = new JdbcSourceConfig();
    JdbcSourceConfig sourceConfig = (JdbcSourceConfig) jdbcSourceConfig.toSourceConfig(json);
    assertNotNull(sourceConfig);
    assertEquals("jdbc", sourceConfig.getName());
    assertEquals("jdbc-output", sourceConfig.getOutput());

    JdbcSourceConfig.JdbcSourceOptions options = sourceConfig.getOptions();
    assertNotNull(options);
    assertEquals("select * from user where age > 10", options.getQuery());

    List<Column> columns = sourceConfig.getColumns();
    assertNotNull(columns);
    assertEquals(3, columns.size());
    Column column = columns.get(0);
    assertEquals("id", column.getName());
    assertEquals("long", column.getType());
  }

  public void testJdbcSourceRead() {}
}
