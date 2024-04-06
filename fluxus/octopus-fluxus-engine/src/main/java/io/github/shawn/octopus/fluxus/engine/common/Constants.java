package io.github.shawn.octopus.fluxus.engine.common;

public interface Constants {

  interface SourceConstants {
    String PULSAR_SOURCE = "pulsar";
    String ACTIVEMQ_SOURCE = "activemq";
    String RABBITMQ_SOURCE = "rabbitmq";
    String JDBC = "jdbc";
    String FILE = "file";
  }

  interface TransformConstants {
    String JSON_PARSE_TRANSFORM = "json-parse";
    String DESENSITIZE_PARSE_TRANSFORM = "desensitize-parse";
    String XML_PARSE_TRANSFORM = "xml-parse";
    String VALUE_MAPPER_TRANSFORM = "value-mapper";
    String EXPRESSION_TRANSFORM = "expression";

    String GENERATED_FILED_TRANSFORM = "generated-field";
  }

  interface SinkConstants {
    String CONSOLE_SINK = "console";
    String JDBC_SINK = "jdbc";
    String DORIS_SINK = "doris";
    String FILE = "file";
    String PULSAR = "pulsar";
  }

  interface JdbcConstant {
    String MYSQL = "mysql";
  }
}
