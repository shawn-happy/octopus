package com.octopus.kettlex.core.runtime;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.steps.StepConfigChannelCombination;
import com.octopus.kettlex.core.steps.StepFactory;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.model.RuntimeConfig;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.reader.RowGeneratorConfig;
import com.octopus.kettlex.model.writer.LogMessageConfig;
import com.octopus.kettlex.runtime.TaskGroup;
import com.octopus.kettlex.runtime.reader.RowGenerator;
import com.octopus.kettlex.runtime.writer.LogMessage;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class ExecuteTests {

  @Test
  public void simpleExecute() throws Exception {
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

    TaskGroup taskGroup = new TaskGroup(configuration);
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
}
