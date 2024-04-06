package io.github.shawn.octopus.data.fluxus.engine.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.io.Resources;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.config.PipelineStatus;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class PipelineTests {
  @Test
  public void test() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/job/pulsar-job.json")),
            StandardCharsets.UTF_8);
    JobConfig jobConfig = new JobConfig(json);
    PipelineGenerator pipelineGenerator = new PipelineGenerator(jobConfig);
    Pipeline pipeline = pipelineGenerator.generate();
    //    PipelineStatus status = pipeline.getStatus();
    //    assertEquals(PipelineStatus.CREATING, status);
    pipeline.prepare();
    //    assertEquals(PipelineStatus.PREPARE, pipeline.getStatus());
    //    //    pipeline.run();
    Thread thread = new Thread(pipeline);
    thread.start();
    thread.join();
  }

  @Test
  public void testBatch() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/job/simple-batch-job.json")),
            StandardCharsets.UTF_8);
    JobConfig jobConfig = new JobConfig(json);
    PipelineGenerator pipelineGenerator = new PipelineGenerator(jobConfig);
    Pipeline pipeline = pipelineGenerator.generate();
    PipelineStatus status = pipeline.getStatus();
    assertEquals(PipelineStatus.CREATING, status);
    pipeline.prepare();
    assertEquals(PipelineStatus.PREPARE, pipeline.getStatus());
    Thread thread = new Thread(pipeline);
    thread.start();
    thread.join();
  }
}
