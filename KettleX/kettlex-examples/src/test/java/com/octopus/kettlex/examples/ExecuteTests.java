package com.octopus.kettlex.examples;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.steps.config.RuntimeConfig;
import com.octopus.kettlex.core.steps.config.TaskConfiguration;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.reader.rowgenerator.RowGeneratorConfig;
import com.octopus.kettlex.runtime.Engine;
import com.octopus.kettlex.runtime.StepConfigChannelCombination;
import com.octopus.kettlex.runtime.TaskGroup;
import com.octopus.kettlex.runtime.executor.Scheduler;
import com.octopus.kettlex.steps.LogMessageConfig;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExecuteTests {

  private static TaskGroup taskGroup;
  private static TaskConfiguration configuration;

  @BeforeAll
  public static void init() throws Exception {
    RowGeneratorConfig meta =
        JsonUtil.fromJson(
                IOUtils.toString(
                    Resources.getResource("steps/read/rowGenerator.json"), StandardCharsets.UTF_8),
                new TypeReference<RowGeneratorConfig>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));

    LogMessageConfig logMessageConfig =
        JsonUtil.fromJson(
                IOUtils.toString(
                    Resources.getResource("steps/writer/logMessage.json"), StandardCharsets.UTF_8),
                new TypeReference<LogMessageConfig>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));

    configuration =
        TaskConfiguration.builder()
            .taskId("simple_test")
            .taskName("simple_test")
            .description("simple test")
            .Version(new Version(1, 0, 0, null, null, null))
            .readers(Collections.singletonList(meta))
            .writers(Collections.singletonList(logMessageConfig))
            .runtimeConfig(RuntimeConfig.builder().channelCapcacity(10).build())
            .build();

    taskGroup = new TaskGroup(configuration);
  }

  @Test
  public void simpleExecute() throws Exception {
    StepConfigChannelCombination rowGeneratorCombination =
        taskGroup.getStepChannel("rowGeneratorTest");
    StepConfigChannelCombination logMessageCombination = taskGroup.getStepChannel("logMessage");

    //    RowGenerator rowGenerator = (RowGenerator)
    // stepFacotry.createStep(rowGeneratorCombination);
    //    LogMessage logMessage = (LogMessage) stepFacotry.createStep(logMessageCombination);
    //    rowGenerator.init();
    //    logMessage.init();
    //    rowGenerator.read();
    //    logMessage.writer();
  }

  @Test
  public void testThreadExecute() throws Exception {
    Scheduler scheduler = new Scheduler();
    scheduler.startTaskGroup(configuration);
  }

  @Test
  public void testEngineStart() throws Exception {
    String configBase64 =
        Base64.getEncoder()
            .encodeToString(
                IOUtils.toString(Resources.getResource("simple.json"), StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8));
    Engine engine = new Engine();
    engine.start(configBase64);
    TimeUnit.SECONDS.sleep(2);
  }
}
