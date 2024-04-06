package io.github.shawn.octopus.fluxus.engine.model.type;

import static com.google.common.base.Preconditions.checkArgument;

import io.github.shawn.octopus.fluxus.api.model.type.CompositeFieldType;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;

@Getter
public final class RowFieldType implements CompositeFieldType {

  private final String[] fieldNames;
  private final DataWorkflowFieldType[] fieldTypes;

  public RowFieldType(String[] fieldNames, DataWorkflowFieldType[] fieldTypes) {
    checkArgument(
        fieldNames.length == fieldTypes.length,
        "The number of field names must be the same as the number of field types.");
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public List<DataWorkflowFieldType> getCompositeType() {
    return Arrays.asList(fieldTypes);
  }

  @Override
  public Class<?> getTypeClass() {
    return RowFieldType.class;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RowFieldType)) {
      return false;
    }
    RowFieldType that = (RowFieldType) obj;
    return Arrays.equals(fieldNames, that.fieldNames) && Arrays.equals(fieldTypes, that.fieldTypes);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(fieldNames);
    result = 31 * result + Arrays.hashCode(fieldTypes);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("ROW<");
    for (int i = 0; i < fieldNames.length; i++) {
      if (i > 0) {
        builder.append(",");
      }
      builder.append(fieldNames[i]).append(" ").append(fieldTypes[i]);
    }
    return builder.append(">").toString();
  }

  public int indexOf(String fieldName) {
    for (int i = 0; i < fieldNames.length; i++) {
      if (fieldNames[i].equals(fieldName)) {
        return i;
      }
    }
    throw new IllegalArgumentException(String.format("can't find field [%s]", fieldName));
  }

  public String getFieldName(int index) {
    return fieldNames[index];
  }

  public DataWorkflowFieldType getFieldType(int index) {
    return fieldTypes[index];
  }
}
