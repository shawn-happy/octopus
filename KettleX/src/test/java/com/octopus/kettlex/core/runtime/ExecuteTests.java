package com.octopus.kettlex.core.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.octopus.kettlex.core.channel.DefaultChannel;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.row.RecordExchanger;
import com.octopus.kettlex.core.row.record.DefaultRecordExchanger;
import com.octopus.kettlex.core.steps.StepFactory;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.model.reader.RowGeneratorConfig;
import com.octopus.kettlex.model.writer.LogMessageConfig;
import com.octopus.kettlex.runtime.reader.RowGenerator;
import com.octopus.kettlex.runtime.writer.LogMessage;
import java.nio.charset.StandardCharsets;
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
    RowGenerator rowGenerator = (RowGenerator) StepFactory.createStep(meta);

    LogMessageConfig logMessageConfig =
        JsonUtil.fromJson(
                IOUtils.toString(
                    Resources.getResource("steps/writer/logMessage.json"), StandardCharsets.UTF_8),
                new TypeReference<LogMessageConfig>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
    LogMessage logMessage = (LogMessage) StepFactory.createStep(logMessageConfig);

    RecordExchanger recordExchanger = new DefaultRecordExchanger(new DefaultChannel("id"));
    rowGenerator.read(recordExchanger);
    logMessage.writer(recordExchanger);
  }
}
