package io.github.shawn.octopus.fluxus.engine.connector.source.jdbc;

import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.Schema;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DateTimeFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DecimalFieldType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class ResultSetConverter implements RowRecordConverter<ResultSet> {

  private final List<Schema> schemas;

  public ResultSetConverter(List<Schema> schemas) {
    this.schemas = schemas;
  }

  @Override
  public SourceRowRecord convert(ResultSet resultSet) {
    Object[] values = new Object[schemas.size()];
    String[] fieldNames = new String[schemas.size()];
    DataWorkflowFieldType[] dataWorkflowFieldTypes = new DataWorkflowFieldType[schemas.size()];
    try {
      for (int i = 0; i < schemas.size(); i++) {
        Schema schema = schemas.get(i);
        String fieldName = schema.getFieldName();
        DataWorkflowFieldType fieldType = schema.getFieldType();
        values[i] = getValue(fieldName, fieldType, resultSet);
        fieldNames[i] = fieldName;
        dataWorkflowFieldTypes[i] = fieldType;
      }
      return SourceRowRecord.builder()
          .fieldNames(fieldNames)
          .fieldTypes(dataWorkflowFieldTypes)
          .values(values)
          .build();

    } catch (SQLException e) {
      throw new DataWorkflowException(e);
    }
  }

  private Object getValue(String fieldName, DataWorkflowFieldType fieldType, ResultSet resultSet)
      throws SQLException {
    Object value = null;
    if (fieldType instanceof BasicFieldType) {
      BasicFieldType basicFieldType = (BasicFieldType) fieldType;
      switch (basicFieldType) {
        case BOOLEAN:
          value = resultSet.getBoolean(fieldName);
          break;
        case TINYINT:
          value = resultSet.getByte(fieldName);
          break;
        case SMALLINT:
          value = resultSet.getShort(fieldName);
          break;
        case INT:
          value = resultSet.getInt(fieldName);
          break;
        case BIGINT:
          value = resultSet.getLong(fieldName);
          break;
        case FLOAT:
          value = resultSet.getFloat(fieldName);
          break;
        case DOUBLE:
          value = resultSet.getDouble(fieldName);
          break;
        case STRING:
          value = resultSet.getString(fieldName);
          break;
      }
    } else if (fieldType instanceof DecimalFieldType) {
      value = resultSet.getBigDecimal(fieldName);
    } else if (fieldType instanceof DateTimeFieldType) {
      DateTimeFieldType dateTimeFieldType = (DateTimeFieldType) fieldType;
      switch (dateTimeFieldType) {
        case DATE_TYPE:
          value = resultSet.getDate(fieldName).toLocalDate();
          break;
        case DATE_TIME_TYPE:
          value = resultSet.getTimestamp(fieldName).toLocalDateTime();
          break;
        case TIME_TYPE:
          value = resultSet.getTime(fieldName).toLocalTime();
          break;
      }
    }
    return value;
  }
}
