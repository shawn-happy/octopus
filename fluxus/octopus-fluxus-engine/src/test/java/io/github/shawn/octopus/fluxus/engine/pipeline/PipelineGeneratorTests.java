package io.github.shawn.octopus.data.fluxus.engine.pipeline;

import com.google.common.io.Resources;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.dag.LogicalDag;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.dag.LogicalEdge;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.dag.LogicalVertex;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PipelineGeneratorTests {
  @Test
  public void testLogicalDag() throws Exception {
    JobConfig jobConfig =
        new JobConfig(
            Resources.toString(
                Objects.requireNonNull(
                    Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("example/job/simple-streaming-job.json")),
                StandardCharsets.UTF_8));
    LogicalDag logicalDag = new LogicalDag(jobConfig);
    Map<String, LogicalVertex> nodes = logicalDag.getNodes();
    List<LogicalEdge> edges = logicalDag.getEdges();
    Assertions.assertEquals(3, nodes.size());
    Assertions.assertEquals(2, edges.size());
  }
}
