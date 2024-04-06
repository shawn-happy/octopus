package io.github.shawn.octopus.fluxus.engine.common.file.format;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.ArrayFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DateTimeFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DecimalFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.MapFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public final class JacksonFormatters {

  public static final DateTimeFormatter TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  public static JacksonToObjectFormatter createJacksonToObjectFormatter(
      @NotNull DataWorkflowFieldType fieldType) {
    if (fieldType instanceof RowFieldType) {
      return createJacksonToRowFormatter((RowFieldType) fieldType);
    } else if (fieldType instanceof BasicFieldType) {
      BasicFieldType basicFieldType = (BasicFieldType) fieldType;
      switch (basicFieldType) {
        case TINYINT:
          return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
        case SMALLINT:
          return jsonNode -> Short.parseShort(jsonNode.asText().trim());
        case INT:
          return JacksonFormatters::convertToInt;
        case BIGINT:
          return JacksonFormatters::convertToLong;
        case FLOAT:
          return JacksonFormatters::convertToFloat;
        case DOUBLE:
          return JacksonFormatters::convertToDouble;
        case BOOLEAN:
          return JacksonFormatters::convertToBoolean;
        case STRING:
          return JacksonFormatters::convertToString;
      }
    } else if (fieldType instanceof DateTimeFieldType) {
      DateTimeFieldType dateTimeFieldType = (DateTimeFieldType) fieldType;
      switch (dateTimeFieldType) {
        case TIME_TYPE:
          return JacksonFormatters::convertToLocalTime;
        case DATE_TYPE:
          return JacksonFormatters::convertToLocalDate;
        case DATE_TIME_TYPE:
          return JacksonFormatters::convertToLocalDateTime;
      }
    } else if (fieldType instanceof DecimalFieldType) {
      return JacksonFormatters::convertToBigDecimal;
    } else if (fieldType instanceof ArrayFieldType) {
      ArrayFieldType arrayFieldType = (ArrayFieldType) fieldType;
      return createJacksonToArrayFormatter(arrayFieldType);
    } else if (fieldType instanceof MapFieldType) {
      MapFieldType mapFieldType = (MapFieldType) fieldType;
      return createJacksonToMapFormatter(mapFieldType);
    }
    throw new DataWorkflowException(
        String.format("The field type [%s] is not supported", fieldType));
  }

  public static ObjectToJacksonFormatter createObjectToJacksonFormatter(
      @NotNull DataWorkflowFieldType fieldType) {
    if (fieldType instanceof RowFieldType) {
      return createRowToJacksonFormatter((RowFieldType) fieldType);
    } else if (fieldType instanceof BasicFieldType) {
      BasicFieldType basicFieldType = (BasicFieldType) fieldType;
      switch (basicFieldType) {
        case TINYINT:
          return (mapper, node, value) -> mapper.getNodeFactory().numberNode((byte) value);
        case SMALLINT:
          return (mapper, node, value) -> mapper.getNodeFactory().numberNode((short) value);
        case INT:
          return (mapper, node, value) -> mapper.getNodeFactory().numberNode((int) value);
        case BIGINT:
          return (mapper, node, value) -> mapper.getNodeFactory().numberNode((long) value);
        case FLOAT:
          return (mapper, node, value) -> mapper.getNodeFactory().numberNode((float) value);
        case DOUBLE:
          return (mapper, node, value) -> mapper.getNodeFactory().numberNode((double) value);
        case BOOLEAN:
          return (mapper, node, value) -> mapper.getNodeFactory().booleanNode((boolean) value);
        case STRING:
          return (mapper, node, value) -> mapper.getNodeFactory().textNode((String) value);
      }
    } else if (fieldType instanceof DateTimeFieldType) {
      DateTimeFieldType dateTimeFieldType = (DateTimeFieldType) fieldType;
      switch (dateTimeFieldType) {
        case TIME_TYPE:
          return (mapper, node, value) ->
              mapper.getNodeFactory().textNode(TIME_FORMAT.format((LocalTime) value));
        case DATE_TYPE:
          return (mapper, node, value) ->
              mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format((LocalDate) value));
        case DATE_TIME_TYPE:
          return (mapper, node, value) ->
              mapper.getNodeFactory().textNode(ISO_LOCAL_DATE_TIME.format((LocalDateTime) value));
      }
    } else if (fieldType instanceof DecimalFieldType) {
      return (mapper, node, value) -> mapper.getNodeFactory().numberNode((BigDecimal) value);
    } else if (fieldType instanceof ArrayFieldType) {
      ArrayFieldType arrayFieldType = (ArrayFieldType) fieldType;
      return createArrayToJacksonFormatter(arrayFieldType);
    } else if (fieldType instanceof MapFieldType) {
      MapFieldType mapFieldType = (MapFieldType) fieldType;
      return createMapToJacksonFormatter(mapFieldType);
    }
    throw new DataWorkflowException(
        String.format("The field type [%s] is not supported", fieldType));
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
    TemporalAccessor parsedTimestamp = ISO_LOCAL_DATE_TIME.parse(jsonNode.asText());
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

  private static JacksonToObjectFormatter createJacksonToArrayFormatter(ArrayFieldType type) {
    JacksonToObjectFormatter valueConverter =
        createJacksonToObjectFormatter(type.getElementFieldType());
    return jsonNode -> {
      Object arr = Array.newInstance(type.getElementFieldType().getTypeClass(), jsonNode.size());
      for (int i = 0; i < jsonNode.size(); i++) {
        Array.set(arr, i, valueConverter.format(jsonNode.get(i)));
      }
      return arr;
    };
  }

  private static JacksonToObjectFormatter createJacksonToMapFormatter(MapFieldType type) {
    JacksonToObjectFormatter valueConverter =
        createJacksonToObjectFormatter(type.getCompositeType().get(1));
    return jsonNode -> {
      Map<Object, Object> value = new HashMap<>();
      jsonNode
          .fields()
          .forEachRemaining(
              entry -> value.put(entry.getKey(), valueConverter.format(entry.getValue())));
      return value;
    };
  }

  private static JacksonToObjectFormatter createJacksonToRowFormatter(RowFieldType rowType) {
    final JacksonToObjectFormatter[] fieldConverters =
        Arrays.stream(rowType.getFieldTypes())
            .map(JacksonFormatters::createJacksonToObjectFormatter)
            .toArray(JacksonToObjectFormatter[]::new);
    final String[] fieldNames = rowType.getFieldNames();
    final DataWorkflowFieldType[] fieldTypes = rowType.getFieldTypes();
    return jsonNode -> {
      int arity = fieldNames.length;
      SourceRowRecord.SourceRowRecordBuilder builder = SourceRowRecord.builder();
      builder.fieldNames(fieldNames);
      builder.fieldTypes(fieldTypes);
      Object[] rows = new Object[arity];
      for (int i = 0; i < arity; i++) {
        String fieldName = fieldNames[i];
        JsonNode field;
        if (jsonNode.isArray()) {
          field = jsonNode.get(i);
        } else {
          field = jsonNode.get(fieldName);
        }
        try {
          Object convertedField = convertField(fieldConverters[i], fieldName, field);
          rows[i] = convertedField;
        } catch (Throwable t) {
          throw new DataWorkflowException(
              String.format("Fail to deserialize at field: %s.", fieldName), t);
        }
      }
      return builder.values(rows).build();
    };
  }

  private static ObjectToJacksonFormatter createRowToJacksonFormatter(RowFieldType rowType) {
    final ObjectToJacksonFormatter[] formatters =
        Arrays.stream(rowType.getFieldTypes())
            .map(JacksonFormatters::createObjectToJacksonFormatter)
            .toArray(ObjectToJacksonFormatter[]::new);
    final String[] fieldNames = rowType.getFieldNames();
    final int arity = fieldNames.length;

    return (mapper, reuse, value) -> {
      ObjectNode node;
      // reuse could be a NullNode if last record is null.
      if (reuse == null || reuse.isNull()) {
        node = mapper.createObjectNode();
      } else {
        node = (ObjectNode) reuse;
      }
      RowRecord row = (RowRecord) value;
      Object[] values = row.pollNext();
      for (int i = 0; i < arity; i++) {
        String fieldName = fieldNames[i];
        node.set(fieldName, formatters[i].format(mapper, node.get(fieldName), values[i]));
      }
      return node;
    };
  }

  private static ObjectToJacksonFormatter createArrayToJacksonFormatter(ArrayFieldType arrayType) {
    final ObjectToJacksonFormatter formatter =
        createObjectToJacksonFormatter(arrayType.getElementFieldType());
    return (mapper, node, value) -> {
      ArrayNode arrayNode;
      // reuse could be a NullNode if last record is null.
      if (node == null || node.isNull()) {
        arrayNode = mapper.createArrayNode();
      } else {
        arrayNode = (ArrayNode) node;
        arrayNode.removeAll();
      }
      Object[] arrayData = (Object[]) value;
      for (Object element : arrayData) {
        arrayNode.add(formatter.format(mapper, null, element));
      }
      return node;
    };
  }

  private static ObjectToJacksonFormatter createMapToJacksonFormatter(MapFieldType mapFieldType) {
    List<DataWorkflowFieldType> compositeType = mapFieldType.getCompositeType();
    DataWorkflowFieldType keyType = compositeType.get(0);
    DataWorkflowFieldType valueType = compositeType.get(1);
    if (!String.class.equals(keyType.getTypeClass())) {
      throw new DataWorkflowException(
          "JSON format doesn't support non-string as key type of map. The type is: "
              + mapFieldType);
    }

    final ObjectToJacksonFormatter valueConverter = createObjectToJacksonFormatter(valueType);
    return (mapper, reuse, value) -> {
      ObjectNode node;

      // reuse could be a NullNode if last record is null.
      if (reuse == null || reuse.isNull()) {
        node = mapper.createObjectNode();
      } else {
        node = (ObjectNode) reuse;
        node.removeAll();
      }

      @SuppressWarnings("unchecked")
      Map<String, ?> mapData = (Map<String, ?>) value;
      for (Map.Entry<String, ?> entry : mapData.entrySet()) {
        String fieldName = entry.getKey();
        node.set(fieldName, valueConverter.format(mapper, node.get(fieldName), entry.getValue()));
      }

      return node;
    };
  }

  private static Object convertField(
      JacksonToObjectFormatter jacksonFormatter, String fieldName, JsonNode field) {
    if (field == null) {
      throw new DataWorkflowException(
          String.format("Could not find field with name %s .", fieldName));
    } else {
      return jacksonFormatter.format(field);
    }
  }
}
