package com.octopus.kettlex.core.steps.read;

import static com.octopus.kettlex.core.steps.StepType.ROW_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.steps.StepFactory;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.reader.RowGeneratorConfig;
import com.octopus.kettlex.model.reader.RowGeneratorConfig.RowGeneratorOptions;
import com.octopus.kettlex.runtime.TaskGroup;
import com.octopus.kettlex.runtime.reader.RowGenerator;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class RowGeneratorTests {

  @Test
  public void createRowGeneratorMeta() throws Exception {
    RowGeneratorConfig meta =
        JsonUtil.fromJson(
                IOUtils.toString(
                    Resources.getResource("steps/read/rowGenerator.json"), StandardCharsets.UTF_8),
                new TypeReference<RowGeneratorConfig>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
    assertNotNull(meta);
    assertEquals(ROW_GENERATOR, meta.getType());
    assertEquals("rowGeneratorTest", meta.getName());
    RowGeneratorOptions options = meta.getOptions();

    assertNotNull(options.getFields());
    assertEquals(2, options.getFields().length);

    assertEquals("id", options.getFields()[0].getName());
    assertEquals(FieldType.String, options.getFields()[0].getFieldType());
    assertEquals("1", options.getFields()[0].getValue());
    assertNull(options.getFields()[0].getFormat());
    assertNull(options.getFields()[0].getLength());

    assertEquals("date", options.getFields()[1].getName());
    assertEquals(FieldType.Date, options.getFields()[1].getFieldType());
    assertEquals("2023-05-18 13:14:15", options.getFields()[1].getValue());
    assertEquals("yyyy-MM-dd HH:mm:ss", options.getFields()[1].getFormat());
    assertEquals(13, options.getFields()[1].getLength());
  }

  @Test
  public void read() throws Exception {
    RowGeneratorConfig meta =
        JsonUtil.fromJson(
                IOUtils.toString(
                    Resources.getResource("steps/read/rowGenerator.json"), StandardCharsets.UTF_8),
                new TypeReference<RowGeneratorConfig>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));

    TaskGroup taskGroup = new TaskGroup(TaskConfiguration.builder().build());
    RowGenerator rowGenerator =
        (RowGenerator) StepFactory.createStep(taskGroup.getStepChannel("rowGeneratorTest"));
    rowGenerator.read();
  }
}
