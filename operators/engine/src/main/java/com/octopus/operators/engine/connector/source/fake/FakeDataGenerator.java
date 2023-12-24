package com.octopus.operators.engine.connector.source.fake;

import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import com.octopus.operators.engine.exception.CommonExceptionConstant;
import com.octopus.operators.engine.exception.EngineException;
import com.octopus.operators.engine.table.EngineRow;
import com.octopus.operators.engine.table.type.ArrayDataType;
import com.octopus.operators.engine.table.type.DateDataType;
import com.octopus.operators.engine.table.type.DecimalDataType;
import com.octopus.operators.engine.table.type.MapDataType;
import com.octopus.operators.engine.table.type.PrimitiveDataType;
import com.octopus.operators.engine.table.type.RowDataType;
import com.octopus.operators.engine.table.type.RowDataTypeParse;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

public class FakeDataGenerator {

  private final FakeSourceOptions options;

  public FakeDataGenerator(FakeSourceOptions options) {
    this.options = options;
  }

  public EngineRow random() {
    FakeSourceRow[] fields = options.getFields();
    EngineRow rows = RowDataTypeParse.toEngineRow(fields);
    RowDataType[] fieldTypes = rows.getFieldTypes();
    Object[] values = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      values[i] = randomValueByType(fieldTypes[i], fields[i]);
    }
    rows.setFieldValues(values);
    return rows;
  }

  public static Object randomValueByType(RowDataType dataType, FakeSourceRow fakeSourceRow) {
    if (dataType instanceof ArrayDataType) {
      ArrayDataType arrayDataType = (ArrayDataType) dataType;
      Integer arraySize = fakeSourceRow.getArraySize();
      Object[] values = new Object[arraySize];
      for (int i = 0; i < arraySize; i++) {
        RowDataType elementClass = arrayDataType.getElementClass();
        values[i] = randomValueByType(elementClass, fakeSourceRow);
      }
      return values;
    } else if (dataType instanceof MapDataType) {
      MapDataType mapDataType = (MapDataType) dataType;
      Integer mapSize = fakeSourceRow.getMapSize();
      Map<Object, Object> map = new HashMap<>();
      RowDataType keyRowDataType = mapDataType.getKeyRowDataType();
      RowDataType valueRowDataType = mapDataType.getValueRowDataType();
      for (int i = 0; i < mapSize; i++) {
        Object key = randomValueByType(keyRowDataType, fakeSourceRow);
        Object value = randomValueByType(valueRowDataType, fakeSourceRow);
        map.put(key, value);
      }
      return map;
    } else if (dataType instanceof DecimalDataType) {
      DecimalDataType decimalDataType = (DecimalDataType) dataType;
      int decimalPrecision = decimalDataType.getPrecision();
      int decimalScale = decimalDataType.getScale();
      return randomBigDecimal(decimalPrecision, decimalScale);
    } else if (dataType instanceof DateDataType) {
      DateDataType dateDataType = (DateDataType) dataType;
      switch (dateDataType) {
        case DATE_TYPE:
          return randomLocalDate();
        case DATE_TIME_TYPE:
          return randomLocalDateTime();
        case TIME_TYPE:
          return randomLocalTime();
        default:
          throw new EngineException(
              CommonExceptionConstant.unsupportedDataType(dateDataType.name()));
      }
    } else if (dataType instanceof PrimitiveDataType) {
      PrimitiveDataType primitiveDataType = (PrimitiveDataType) dataType;
      switch (primitiveDataType) {
        case BOOLEAN:
          return randomBoolean();
        case TINYINT:
          return randomTinyint(
              fakeSourceRow.getTinyIntTemplate(),
              fakeSourceRow.getTinyIntMin(),
              fakeSourceRow.getTinyIntMax());
        case SMALLINT:
          return randomSmallint(
              fakeSourceRow.getSmallIntTemplate(),
              fakeSourceRow.getSmallIntMin(),
              fakeSourceRow.getSmallIntMax());
        case INT:
          return randomInt(
              fakeSourceRow.getIntTemplate(), fakeSourceRow.getIntMin(), fakeSourceRow.getIntMax());
        case BIGINT:
          return randomBigint(
              fakeSourceRow.getBigIntTemplate(),
              fakeSourceRow.getBigIntMin(),
              fakeSourceRow.getBigIntMax());
        case FLOAT:
          return randomFloat(
              fakeSourceRow.getFloatTemplate(),
              fakeSourceRow.getFloatMin(),
              fakeSourceRow.getFloatMax());
        case DOUBLE:
          return randomDouble(
              fakeSourceRow.getDoubleTemplate(),
              fakeSourceRow.getDoubleMin(),
              fakeSourceRow.getDoubleMax());
        case STRING:
          return randomString(fakeSourceRow.getStringTemplate(), fakeSourceRow.getStringLength());
        default:
          throw new EngineException(
              CommonExceptionConstant.unsupportedDataType(primitiveDataType.name()));
      }
    }

    return null;
  }

  public static boolean randomBoolean() {
    return RandomUtils.nextInt(0, 2) == 1;
  }

  public static byte randomTinyint(List<Integer> templates, Integer min, Integer max) {
    Integer value = randomFromList(templates);
    if (value != null) {
      return value.byteValue();
    }
    return (byte) RandomUtils.nextInt(min, max);
  }

  public static short randomSmallint(List<Integer> templates, Integer min, Integer max) {
    Integer value = randomFromList(templates);
    if (value != null) {
      return value.shortValue();
    }
    return (short) RandomUtils.nextInt(min, max);
  }

  public static int randomInt(List<Integer> templates, Integer min, Integer max) {
    Integer value = randomFromList(templates);
    if (value != null) {
      return value;
    }
    return RandomUtils.nextInt(min, max);
  }

  public static long randomBigint(List<Long> templates, Long min, Long max) {
    Long value = randomFromList(templates);
    if (value != null) {
      return value;
    }
    return RandomUtils.nextLong(min, max);
  }

  public static float randomFloat(List<Float> templates, Float min, Float max) {
    Float value = randomFromList(templates);
    if (value != null) {
      return value;
    }
    return RandomUtils.nextFloat(min, max);
  }

  public static double randomDouble(List<Double> templates, Double min, Double max) {
    Double value = randomFromList(templates);
    if (value != null) {
      return value;
    }
    return RandomUtils.nextDouble(min, max);
  }

  public static LocalDate randomLocalDate() {
    return randomLocalDateTime().toLocalDate();
  }

  public static LocalTime randomLocalTime() {
    return randomLocalDateTime().toLocalTime();
  }

  public static LocalDateTime randomLocalDateTime() {
    int year = RandomUtils.nextInt(2000, LocalDateTime.now().getYear() + 1);
    int month = RandomUtils.nextInt(1, 13);
    int day = month == 2 ? RandomUtils.nextInt(1, 29) : RandomUtils.nextInt(1, 31);
    int hour = RandomUtils.nextInt(0, 24);
    int minute = RandomUtils.nextInt(0, 60);
    int second = RandomUtils.nextInt(0, 60);
    return LocalDateTime.of(year, month, day, hour, minute, second);
  }

  public static BigDecimal randomBigDecimal(int precision, int scale) {
    return new BigDecimal(
        RandomStringUtils.randomNumeric(precision - scale)
            + "."
            + RandomStringUtils.randomNumeric(scale));
  }

  public static String randomString(List<String> stringTemplate, int stringLength) {
    if (!CollectionUtils.isEmpty(stringTemplate)) {
      return randomFromList(stringTemplate);
    }
    return RandomStringUtils.randomAlphabetic(stringLength);
  }

  public static <T> T randomFromList(List<T> list) {
    if (CollectionUtils.isEmpty(list)) {
      return null;
    }
    return list.get(RandomUtils.nextInt(0, list.size() - 1));
  }
}
