package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.utils.YamlUtil;
import com.octopus.kettlex.runtime.config.JobConfiguration;
import com.octopus.kettlex.runtime.executor.Scheduler;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Engine {

  public static void main(String[] args) throws Exception {
    String configBase64 = args[0];
    log.info("spark runtime instance base64: {}", configBase64);
    Engine engine = new Engine();
    engine.start(configBase64);
  }

  public void start(String configBase64) throws Exception {
    Scheduler scheduler = new Scheduler();
    scheduler.startTaskGroup(buildJobConfiguration(configBase64));
  }

  public JobConfiguration buildJobConfiguration(@Nonnull String configBase64) {
    JobConfiguration jobConfiguration = new JobConfiguration();
    YamlUtil.toYamlNode(Base64.getDecoder().decode(configBase64.getBytes(StandardCharsets.UTF_8)))
        .ifPresent(jsonNode -> jobConfiguration.loadYaml(jsonNode));
    return jobConfiguration;
  }
}
