package com.octopus.kettlex.core.runtime;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.octopus.kettlex.Engine;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.steps.StepConfigChannelCombination;
import com.octopus.kettlex.core.steps.StepFactory;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.model.RuntimeConfig;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.reader.RowGeneratorConfig;
import com.octopus.kettlex.model.writer.LogMessageConfig;
import com.octopus.kettlex.runtime.TaskGroup;
import com.octopus.kettlex.runtime.executor.DefaultExecutor;
import com.octopus.kettlex.runtime.executor.Executor;
import com.octopus.kettlex.runtime.reader.RowGenerator;
import com.octopus.kettlex.runtime.writer.LogMessage;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExecuteTests {

  private static TaskGroup taskGroup;

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

    TaskConfiguration configuration =
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

    RowGenerator rowGenerator = (RowGenerator) StepFactory.createStep(rowGeneratorCombination);
    LogMessage logMessage = (LogMessage) StepFactory.createStep(logMessageCombination);
    rowGenerator.init();
    logMessage.init();
    rowGenerator.read();
    logMessage.writer();
  }

  @Test
  public void testThreadExecute() throws Exception {
    Executor executor = new DefaultExecutor(taskGroup);
    executor.executor();
    TimeUnit.SECONDS.sleep(2);
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
