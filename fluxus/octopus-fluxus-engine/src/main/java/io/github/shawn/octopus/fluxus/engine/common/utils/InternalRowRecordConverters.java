package io.github.shawn.octopus.fluxus.engine.common.utils;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.Schema;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.ArrayFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DateTimeFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DecimalFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.MapFieldType;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

public class InternalRowRecordConverters {

  private static final String TIME_FORMATTER = "HH:mm:ss.SSS";
  private static final String DATE_FORMATTER = "yyyy-MM-dd";
  private static final String DATE_TIME_FORMATTER = "yyyy-MM-dd HH:mm:ss";

  public static final DateTimeFormatter TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  public static String convertToString(@NotNull DataWorkflowFieldType fieldType, Object value) {
    if (value == null) {
      return null;
    }

    if (fieldType instanceof BasicFieldType) {
      BasicFieldType basicFieldType = (BasicFieldType) fieldType;
      switch (basicFieldType) {
        case TINYINT:
        case SMALLINT:
        case INT:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case STRING:
          return String.valueOf(value);
      }
    } else if (fieldType instanceof DecimalFieldType) {
      return String.valueOf(value);
    } else if (fieldType instanceof DateTimeFieldType) {
      DateTimeFieldType dateTimeFieldType = (DateTimeFieldType) fieldType;
      switch (dateTimeFieldType) {
        case TIME_TYPE:
          return ((LocalTime) value).format(DateTimeFormatter.ofPattern(TIME_FORMATTER));
        case DATE_TYPE:
          return ((LocalDate) value).format(DateTimeFormatter.ofPattern(DATE_FORMATTER));
        case DATE_TIME_TYPE:
          return ((LocalDateTime) value).format(DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));
      }
    } else if (fieldType instanceof ArrayFieldType || fieldType instanceof MapFieldType) {
      return JsonUtils.toJson(value).orElseThrow(() -> new DataWorkflowException("cannot convert"));
    }
    return null;
  }

  public static Integer convertToInt(DataWorkflowFieldType fieldType, Object value) {
    if (value == null) {
      return null;
    }
    if (fieldType instanceof BasicFieldType) {
      BasicFieldType basicFieldType = (BasicFieldType) fieldType;
      switch (basicFieldType) {
        case TINYINT:
        case SMALLINT:
        case INT:
          return Integer.valueOf(value.toString());
        case STRING:
          if (StringUtils.isNumeric(value.toString())) {
            return Integer.valueOf(value.toString());
          }
          throw new DataWorkflowException(
              String.format("string [%s] can not convert to int", value));
        case BIGINT:
          OverFlowUtil.validateIntegerNotOverFlow(Long.parseLong(value.toString()));
          return Long.valueOf(value.toString()).intValue();
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
      }
    }
    return null;
  }

  public static RowRecord jsonConvert(JsonNode jsonNode, List<Schema> schemas) {
    TransformRowRecord rowRecord =
        new TransformRowRecord(
            schemas.stream().map(Schema::getFieldName).toArray(String[]::new),
            schemas.stream().map(Schema::getFieldType).toArray(DataWorkflowFieldType[]::new));
    Object[] records = new Object[schemas.size()];
    for (int i = 0; i < schemas.size(); i++) {
      Schema schema = schemas.get(i);
      DataWorkflowFieldType fieldType = schema.getFieldType();
      String fieldName = schema.getFieldName();
      JsonNode field = jsonNode.get(fieldName);
      if (fieldType instanceof BasicFieldType) {
        BasicFieldType basicFieldType = (BasicFieldType) fieldType;
        switch (basicFieldType) {
          case TINYINT:
            records[i] = Byte.parseByte(field.asText().trim());
            break;
          case SMALLINT:
            records[i] = Short.parseShort(field.asText().trim());
            break;
          case INT:
            records[i] = convertToInt(field);
            break;
          case BIGINT:
            records[i] = convertToLong(field);
            break;
          case FLOAT:
            records[i] = convertToFloat(field);
            break;
          case DOUBLE:
            records[i] = convertToDouble(field);
            break;
          case BOOLEAN:
            records[i] = convertToBoolean(field);
            break;
          case STRING:
            records[i] = convertToString(field);
            break;
        }
      } else if (fieldType instanceof DateTimeFieldType) {
        DateTimeFieldType dateTimeFieldType = (DateTimeFieldType) fieldType;
        switch (dateTimeFieldType) {
          case DATE_TIME_TYPE:
            records[i] = convertToLocalDateTime(jsonNode);
            break;
          case DATE_TYPE:
            records[i] = convertToLocalDate(jsonNode);
            break;
          case TIME_TYPE:
            records[i] = convertToLocalTime(jsonNode);
            break;
        }
      } else if (fieldType instanceof DecimalFieldType) {
        records[i] = convertToBigDecimal(field);
      } else if (fieldType instanceof ArrayFieldType) {
        ArrayFieldType arrayFieldType = (ArrayFieldType) fieldType;
        DataWorkflowFieldType elementFieldType = arrayFieldType.getElementFieldType();
        Object arr = Array.newInstance(elementFieldType.getTypeClass(), jsonNode.size());
        ArrayNode arrayNode = (ArrayNode) field;
        for (int j = 0; j < arrayNode.size(); j++) {
          Array.set(arr, i, jsonConvert(arrayNode.get(j)));
        }
      }
    }
    return null;
  }

  private static Object jsonConvert(JsonNode jsonNode) {
    return null;
  }

  private static boolean convertToBoolean(JsonNode jsonNode) {
    if (jsonNode.isBoolean()) {
      // avoid redundant toString and parseBoolean, for better performance
      return jsonNode.asBoolean();
    } else {
      return Boolean.parseBoolean(jsonNode.asText().trim());
    }
  }

  private static int convertToInt(JsonNode jsonNode) {
    if (jsonNode.canConvertToInt()) {
      // avoid redundant toString and parseInt, for better performance
      return jsonNode.asInt();
    } else {
      return Integer.parseInt(jsonNode.asText().trim());
    }
  }

  private static long convertToLong(JsonNode jsonNode) {
    if (jsonNode.canConvertToLong()) {
      // avoid redundant toString and parseLong, for better performance
      return jsonNode.asLong();
    } else {
      return Long.parseLong(jsonNode.asText().trim());
    }
  }

  private static double convertToDouble(JsonNode jsonNode) {
    if (jsonNode.isDouble()) {
      // avoid redundant toString and parseDouble, for better performance
      return jsonNode.asDouble();
    } else {
      return Double.parseDouble(jsonNode.asText().trim());
    }
  }

  private static float convertToFloat(JsonNode jsonNode) {
    if (jsonNode.isDouble()) {
      // avoid redundant toString and parseDouble, for better performance
      return (float) jsonNode.asDouble();
    } else {
      return Float.parseFloat(jsonNode.asText().trim());
    }
  }

  private static LocalDate convertToLocalDate(JsonNode jsonNode) {
    return ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
  }

  private static LocalTime convertToLocalTime(JsonNode jsonNode) {
    TemporalAccessor parsedTime = TIME_FORMAT.parse(jsonNode.asText());
    return parsedTime.query(TemporalQueries.localTime());
  }

  private static LocalDateTime convertToLocalDateTime(JsonNode jsonNode) {
    TemporalAccessor parsedTimestamp =
        DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(jsonNode.asText());
    LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
    LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
    return LocalDateTime.of(localDate, localTime);
  }

  private static String convertToString(JsonNode jsonNode) {
    if (jsonNode.isContainerNode()) {
      return jsonNode.toString();
    } else {
      return jsonNode.asText();
    }
  }

  private static BigDecimal convertToBigDecimal(JsonNode jsonNode) {
    BigDecimal bigDecimal;
    if (jsonNode.isBigDecimal()) {
      bigDecimal = jsonNode.decimalValue();
    } else {
      bigDecimal = new BigDecimal(jsonNode.asText());
    }

    return bigDecimal;
  }
}
