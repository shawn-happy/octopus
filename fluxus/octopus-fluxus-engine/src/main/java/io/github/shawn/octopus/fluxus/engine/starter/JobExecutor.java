package io.github.shawn.octopus.data.fluxus.engine.starter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.Pipeline;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.PipelineGenerator;
import io.github.shawn.octopus.data.fluxus.engine.pipeline.config.JobConfig;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobExecutor {

  private final JobConfig jobConfig;
  private final ExecutorService executorService;

  public JobExecutor(String json) {
    log.info("job config base64 content: {}", json);
    this.jobConfig =
        new JobConfig(new String(Base64.getDecoder().decode(json), StandardCharsets.UTF_8));
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(String.format("pipeline-%s", jobConfig.getJobId()))
                .build());
  }

  public void execute() {
    PipelineGenerator pipelineGenerator = new PipelineGenerator(jobConfig);
    Pipeline generate = pipelineGenerator.generate();
    generate.prepare();
    executorService.submit(generate);
    executorService.shutdown();
  }

  public boolean await() throws InterruptedException {
    return executorService.awaitTermination(100, TimeUnit.MINUTES);
  }
}
