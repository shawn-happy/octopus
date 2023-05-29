package com.octopus.kettlex.core.steps.read;

import static com.octopus.kettlex.core.steps.StepType.ROW_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.octopus.kettlex.core.channel.DefaultChannel;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.RecordExchanger;
import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.row.record.DefaultRecordExchanger;
import com.octopus.kettlex.core.steps.common.StepFactory;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGenerator;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGeneratorMeta;
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
    assertEquals(ROW_GENERATOR, meta.getStepType());
    assertEquals("rowGeneratorTest", meta.getName());
    assertNotNull(meta.getFields());
    assertTrue(meta.getFields().length == 2);

    assertEquals("id", meta.getFields()[0].getName());
    assertEquals(FieldType.String, meta.getFields()[0].getFieldType());
    assertEquals("1", meta.getFields()[0].getValue());
    assertNull(meta.getFields()[0].getFormat());
    assertNull(meta.getFields()[0].getLength());

    assertEquals("date", meta.getFields()[1].getName());
    assertEquals(FieldType.Date, meta.getFields()[1].getFieldType());
    assertEquals("2023-05-18 13:14:15", meta.getFields()[1].getValue());
    assertEquals("yyyy-MM-dd HH:mm:ss", meta.getFields()[1].getFormat());
    assertEquals(13, meta.getFields()[1].getLength());
  }

  @Test
  public void read() throws Exception {
    RowGeneratorMeta meta =
        JsonUtil.fromJson(
                IOUtils.toString(
                    Resources.getResource("steps/read/rowGenerator.json"), StandardCharsets.UTF_8),
                new TypeReference<RowGeneratorMeta>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
    RowGenerator rowGenerator = (RowGenerator) StepFactory.createStep(meta);
    RecordExchanger recordExchanger = new DefaultRecordExchanger(new DefaultChannel());
    rowGenerator.read(recordExchanger);
    Record fetch = recordExchanger.fetch();
    assertNotNull(fetch);
  }
}
