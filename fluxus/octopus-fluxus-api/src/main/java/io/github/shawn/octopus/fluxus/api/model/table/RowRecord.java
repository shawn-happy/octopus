package io.github.shawn.octopus.fluxus.api.model.table;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public interface RowRecord {
  String[] getFieldNames();

  DataWorkflowFieldType[] getFieldTypes();

  List<Object[]> getValues();

  int size();

  default String getFieldName(int index) {
    String[] fieldNames = getFieldNames();
    if (ArrayUtils.isEmpty(fieldNames)) {
      return null;
    }
    return fieldNames[index];
  }

  default DataWorkflowFieldType getFieldType(int index) {
    DataWorkflowFieldType[] fieldTypes = getFieldTypes();
    if (ArrayUtils.isEmpty(fieldTypes)) {
      return null;
    }
    return fieldTypes[index];
  }

  default int indexOf(String fieldName) {
    String[] fieldNames = getFieldNames();
    if (ArrayUtils.isEmpty(fieldNames)) {
      throw new IllegalArgumentException("fields is empty");
    }
    for (int i = 0; i < fieldNames.length; i++) {
      if (StringUtils.equals(fieldName, fieldNames[i])) {
        return i;
      }
    }
    throw new IllegalArgumentException(String.format("can't find field [%s]", fieldName));
  }

  Object[] pollNext();
}
