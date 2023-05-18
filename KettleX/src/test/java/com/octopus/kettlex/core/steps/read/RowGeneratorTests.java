package com.octopus.kettlex.core.steps.read;

import static com.octopus.kettlex.core.steps.StepType.ROW_GENERATOR_INPUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.steps.read.rowgenerator.RowGeneratorMeta;
import com.octopus.kettlex.core.utils.JsonUtil;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class RowGeneratorTests {

  @Test
  public void createRowGeneratorMeta() throws Exception {
    RowGeneratorMeta meta =
        JsonUtil.fromJson(
                IOUtils.toString(
                    Resources.getResource("steps/read/rowGenerator.json"), StandardCharsets.UTF_8),
                new TypeReference<RowGeneratorMeta>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
    assertNotNull(meta);
    assertEquals(ROW_GENERATOR_INPUT, meta.getStepType());
    assertEquals("rowGeneratorTest", meta.getStepName());
    assertNotNull(meta.getFields());
    assertTrue(meta.getFields().length == 2);

    assertEquals("id", meta.getFields()[0].getName());
    assertEquals("String", meta.getFields()[0].getType());
    assertEquals("1", meta.getFields()[0].getValue());
    assertNull(meta.getFields()[0].getFormat());
    assertNull(meta.getFields()[0].getLength());

    assertEquals("date", meta.getFields()[1].getName());
    assertEquals("Date", meta.getFields()[1].getType());
    assertEquals("2023-05-18 13:14:15", meta.getFields()[1].getValue());
    assertEquals("yyyy-MM-dd HH:mm:ss", meta.getFields()[1].getFormat());
    assertEquals(13, meta.getFields()[1].getLength());
  }
}
