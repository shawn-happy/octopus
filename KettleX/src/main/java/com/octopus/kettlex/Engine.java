package com.octopus.kettlex;

import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.runtime.executor.Scheduler;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Engine {

  public static void main(String[] args) {
    String configBase64 = args[0];
    log.info("spark runtime instance base64: {}", configBase64);
    Engine engine = new Engine();
    engine.start(configBase64);
  }

  public void start(String configBase64) {
    TaskConfiguration taskConfiguration =
        JsonUtil.fromJson(
                Base64.getDecoder().decode(configBase64.getBytes(StandardCharsets.UTF_8)),
                TaskConfiguration.class)
            .orElseThrow(() -> new KettleXJSONException("parse task config error."));
    Scheduler scheduler = new Scheduler();
    scheduler.startTaskGroup(taskConfiguration);
  }
}
