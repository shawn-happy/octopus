package io.github.shawn.octopus.fluxus.engine.pipeline.dag;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.config.Column;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.connector.sink.console.ConsoleSinkConfig;
import io.github.shawn.octopus.fluxus.engine.connector.source.pulsar.PulsarSourceConfig;
import io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse.JsonParseTransformConfig;
import io.github.shawn.octopus.fluxus.engine.connector.transform.valueMapper.MapTransformConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LogicalDagTests {
  @Test
  public void testSimpleDag() {
    LogicalDag logicalDag =
        new LogicalDag(
            JobConfig.builder()
                .sources(
                    Collections.singletonList(
                        PulsarSourceConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("pulsar")
                            .output("pulsar_output")
                            .columns(
                                Arrays.asList(
                                    Column.builder().name("1").type("string").build(),
                                    Column.builder().name("2").type("string").build(),
                                    Column.builder().name("3").type("string").build()))
                            .options(PulsarSourceConfig.PulsarSourceOptions.builder().build())
                            .build()))
                .sink(
                    ConsoleSinkConfig.builder()
                        .id(IdGenerator.uuid())
                        .name("console")
                        .input("pulsar_output")
                        .options(ConsoleSinkConfig.ConsoleSinkOptions.builder().build())
                        .build())
                .build());
    List<LogicalEdge> edges = logicalDag.getEdges();
    assertEquals(1, edges.size());
    Map<String, LogicalVertex> nodes = logicalDag.getNodes();
    assertEquals(2, nodes.size());
  }

  @Test
  public void testErrorDag() {
    Assertions.assertThrows(
        DataWorkflowException.class,
        () ->
            new LogicalDag(
                JobConfig.builder()
                    .sources(
                        Collections.singletonList(
                            PulsarSourceConfig.builder()
                                .id(IdGenerator.uuid())
                                .name("pulsar")
                                .output("pulsar_output")
                                .columns(
                                    Arrays.asList(
                                        Column.builder().name("1").type("string").build(),
                                        Column.builder().name("2").type("string").build(),
                                        Column.builder().name("3").type("string").build()))
                                .options(PulsarSourceConfig.PulsarSourceOptions.builder().build())
                                .build()))
                    .sink(
                        ConsoleSinkConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("console")
                            .input("pulsar")
                            .options(ConsoleSinkConfig.ConsoleSinkOptions.builder().build())
                            .build())
                    .build()));
  }

  @Test
  public void testComplexDag1() {
    LogicalDag logicalDag =
        new LogicalDag(
            JobConfig.builder()
                .sources(
                    Collections.singletonList(
                        PulsarSourceConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("pulsar")
                            .output("pulsar_output")
                            .columns(
                                Arrays.asList(
                                    Column.builder().name("1").type("string").build(),
                                    Column.builder().name("2").type("string").build(),
                                    Column.builder().name("3").type("string").build()))
                            .options(PulsarSourceConfig.PulsarSourceOptions.builder().build())
                            .build()))
                .transforms(
                    Arrays.asList(
                        JsonParseTransformConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("json")
                            .inputs(Collections.singletonList("pulsar_output"))
                            .output("json-parse")
                            .options(
                                JsonParseTransformConfig.JsonParseTransformOptions.builder()
                                    .build())
                            .build(),
                        MapTransformConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("value-mapper")
                            .inputs(Collections.singletonList("json-parse"))
                            .output("map-value")
                            .options(MapTransformConfig.MapTransformOptions.builder().build())
                            .build()))
                .sink(
                    ConsoleSinkConfig.builder()
                        .id(IdGenerator.uuid())
                        .name("console")
                        .input("map-value")
                        .options(ConsoleSinkConfig.ConsoleSinkOptions.builder().build())
                        .build())
                .build());
    List<LogicalEdge> edges = logicalDag.getEdges();
    assertEquals(3, edges.size());
    Map<String, LogicalVertex> nodes = logicalDag.getNodes();
    assertEquals(4, nodes.size());
  }

  @Test
  public void testComplexDag2() {
    LogicalDag logicalDag =
        new LogicalDag(
            JobConfig.builder()
                .sources(
                    Collections.singletonList(
                        PulsarSourceConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("pulsar")
                            .output("pulsar_output")
                            .columns(
                                Arrays.asList(
                                    Column.builder().name("1").type("string").build(),
                                    Column.builder().name("2").type("string").build(),
                                    Column.builder().name("3").type("string").build()))
                            .options(PulsarSourceConfig.PulsarSourceOptions.builder().build())
                            .build()))
                .transforms(
                    Arrays.asList(
                        JsonParseTransformConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("json")
                            .inputs(Collections.singletonList("pulsar_output"))
                            .output("json-parse")
                            .options(
                                JsonParseTransformConfig.JsonParseTransformOptions.builder()
                                    .build())
                            .build(),
                        MapTransformConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("value-mapper")
                            .inputs(Arrays.asList("pulsar_output", "json-parse"))
                            .output("map-value")
                            .options(MapTransformConfig.MapTransformOptions.builder().build())
                            .build()))
                .sink(
                    ConsoleSinkConfig.builder()
                        .id(IdGenerator.uuid())
                        .name("console")
                        .input("map-value")
                        .options(ConsoleSinkConfig.ConsoleSinkOptions.builder().build())
                        .build())
                .build());
    List<LogicalEdge> edges = logicalDag.getEdges();
    assertEquals(4, edges.size());
    Map<String, LogicalVertex> nodes = logicalDag.getNodes();
    assertEquals(4, nodes.size());
  }

  @Test
  public void testComplexDag3() {
    Assertions.assertThrows(
        DataWorkflowException.class,
        () ->
            new LogicalDag(
                JobConfig.builder()
                    .sources(
                        Collections.singletonList(
                            PulsarSourceConfig.builder()
                                .id(IdGenerator.uuid())
                                .name("pulsar")
                                .output("pulsar_output")
                                .columns(
                                    Arrays.asList(
                                        Column.builder().name("1").type("string").build(),
                                        Column.builder().name("2").type("string").build(),
                                        Column.builder().name("3").type("string").build()))
                                .options(PulsarSourceConfig.PulsarSourceOptions.builder().build())
                                .build()))
                    .transforms(
                        Arrays.asList(
                            JsonParseTransformConfig.builder()
                                .id(IdGenerator.uuid())
                                .name("json")
                                .inputs(Collections.singletonList("pulsar_output"))
                                .output("json-parse")
                                .options(
                                    JsonParseTransformConfig.JsonParseTransformOptions.builder()
                                        .build())
                                .build(),
                            MapTransformConfig.builder()
                                .id(IdGenerator.uuid())
                                .name("value-mapper")
                                .inputs(Arrays.asList("pulsar-output", "json-parse"))
                                .output("map-value")
                                .options(MapTransformConfig.MapTransformOptions.builder().build())
                                .build()))
                    .sink(
                        ConsoleSinkConfig.builder()
                            .id(IdGenerator.uuid())
                            .name("console")
                            .input("map-value")
                            .options(ConsoleSinkConfig.ConsoleSinkOptions.builder().build())
                            .build())
                    .build()));
  }
}
