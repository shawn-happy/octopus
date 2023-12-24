package com.octopus.operators.engine.connector.source.fake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import com.octopus.operators.engine.table.type.ArrayDataType;
import com.octopus.operators.engine.table.type.DateDataType;
import com.octopus.operators.engine.table.type.DecimalDataType;
import com.octopus.operators.engine.table.type.MapDataType;
import com.octopus.operators.engine.table.type.PrimitiveDataType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.Test;

public class FakeDataGeneratorTests {

  @Test
  public void testFakeDateGenerators() {
    FakeSourceRow fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("boolean")
            .booleanTemplate(List.of("true", "false"))
            .build();

    Object value = FakeDataGenerator.randomValueByType(PrimitiveDataType.BOOLEAN, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Boolean.class, value.getClass());

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("tinyint")
            .tinyIntMin(0)
            .tinyIntMax(120)
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.TINYINT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Byte.class, value.getClass());
    assertTrue((Byte) value <= 120);

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("tinyint")
            .tinyIntTemplate(List.of(1, 2, 3, 4, 5, 10))
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.TINYINT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Byte.class, value.getClass());
    assertTrue(List.of(1, 2, 3, 4, 5, 10).contains(((Byte) value).intValue()));

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("smallInt")
            .smallIntMin(0)
            .smallIntMax(10)
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.SMALLINT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Short.class, value.getClass());
    assertTrue((Short) value <= 10);

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("smallInt")
            .smallIntTemplate(List.of(1, 10))
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.SMALLINT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Short.class, value.getClass());
    assertTrue(List.of(1, 10).contains(((Short) value).intValue()));

    fakeSourceRow =
        FakeSourceRow.builder().fieldName("a").fieldType("int").intMin(0).intMax(10).build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.INT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Integer.class, value.getClass());
    assertTrue((Integer) value <= 10);

    fakeSourceRow =
        FakeSourceRow.builder().fieldName("a").fieldType("int").intTemplate(List.of(1, 10)).build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.INT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Integer.class, value.getClass());
    assertTrue(List.of(1, 10).contains((Integer) value));

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("bigint")
            .bigIntMin(0L)
            .bigIntMax(10L)
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.BIGINT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Long.class, value.getClass());
    assertTrue((Long) value <= 10L);

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("bigint")
            .bigIntTemplate(List.of(1L, 10L))
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.BIGINT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Long.class, value.getClass());
    assertTrue(List.of(1L, 10L).contains((Long) value));

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("float")
            .floatMin(0.0f)
            .floatMax(10.0f)
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.FLOAT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Float.class, value.getClass());
    assertTrue((Float) value <= 10.0f);

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("float")
            .floatTemplate(List.of(1.0f, 10.0f))
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.FLOAT, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Float.class, value.getClass());
    assertTrue(List.of(1.0f, 10.0f).contains((Float) value));

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("double")
            .doubleMin(0.0d)
            .doubleMax(10.0d)
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.DOUBLE, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Double.class, value.getClass());
    assertTrue((Double) value <= 10.0d);

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("double")
            .doubleTemplate(List.of(1.0d, 10.0d))
            .build();
    value = FakeDataGenerator.randomValueByType(PrimitiveDataType.DOUBLE, fakeSourceRow);
    assertNotNull(value);
    assertEquals(Double.class, value.getClass());
    assertTrue(List.of(1.0d, 10.0d).contains((Double) value));

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("date")
            .day(20)
            .month(10)
            .year(2023)
            .hours(13)
            .minutes(50)
            .seconds(50)
            .build();
    value = FakeDataGenerator.randomValueByType(DateDataType.DATE_TYPE, fakeSourceRow);
    assertNotNull(value);
    assertEquals(LocalDate.class, value.getClass());

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("datetime")
            .day(20)
            .month(10)
            .year(2023)
            .hours(13)
            .minutes(50)
            .seconds(50)
            .build();
    value = FakeDataGenerator.randomValueByType(DateDataType.DATE_TIME_TYPE, fakeSourceRow);
    assertNotNull(value);
    assertEquals(LocalDateTime.class, value.getClass());

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("time")
            .day(20)
            .month(10)
            .year(2023)
            .hours(13)
            .minutes(50)
            .seconds(50)
            .build();
    value = FakeDataGenerator.randomValueByType(DateDataType.TIME_TYPE, fakeSourceRow);
    assertNotNull(value);
    assertEquals(LocalTime.class, value.getClass());

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("map<int, String>")
            .intMin(0)
            .intMax(2)
            .mapSize(3)
            .build();
    value =
        FakeDataGenerator.randomValueByType(
            new MapDataType(PrimitiveDataType.INT, PrimitiveDataType.STRING), fakeSourceRow);
    assertNotNull(value);

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("array<String>")
            .stringTemplate(List.of("a", "b", "c", "d", "e", "f"))
            .arraySize(6)
            .build();
    value = FakeDataGenerator.randomValueByType(ArrayDataType.STRING_ARRAY, fakeSourceRow);
    assertNotNull(value);
    assertEquals(6, ((Object[]) value).length);

    fakeSourceRow =
        FakeSourceRow.builder()
            .fieldName("a")
            .fieldType("decimal<3, 1>")
            .decimalPrecision(3)
            .decimalScale(1)
            .build();
    value = FakeDataGenerator.randomValueByType(new DecimalDataType(3, 1), fakeSourceRow);
    assertNotNull(value);
  }
}
