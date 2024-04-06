package io.github.shawn.octopus.fluxus.engine.model.table;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

/** 多行数据 */
@Getter
public class TransformRowRecord implements RowRecord {
  private String[] fieldNames;
  private DataWorkflowFieldType[] fieldTypes;
  private LinkedList<Object[]> values;
  private int size;

  public TransformRowRecord(String[] fieldNames, DataWorkflowFieldType[] fieldTypes) {
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  public List<Object[]> getValues() {
    return new ArrayList<>(values);
  }

  public void addRecords(List<Object[]> records) {
    if (values == null) {
      values = new LinkedList<>();
    }
    values.addAll(records);
    size += records.size();
  }

  public void addRecord(Object[] record) {
    if (values == null) {
      values = new LinkedList<>();
    }
    values.add(record);
    size++;
  }

  public void addColumn(String fieldName, DataWorkflowFieldType fieldType) {
    if (ArrayUtils.isEmpty(fieldNames) && ArrayUtils.isEmpty(fieldTypes)) {
      fieldNames = new String[] {fieldName};
      fieldTypes = new DataWorkflowFieldType[] {fieldType};
      return;
    }
    if (ArrayUtils.contains(this.fieldNames, fieldName)) {
      throw new DataWorkflowException(String.format("Duplicate field [%s]", fieldName));
    }
    String[] newFieldNames = new String[fieldNames.length + 1];
    System.arraycopy(fieldNames, 0, newFieldNames, 0, fieldNames.length);
    newFieldNames[fieldNames.length] = fieldName;
    DataWorkflowFieldType[] newFieldTypes = new DataWorkflowFieldType[fieldTypes.length + 1];
    System.arraycopy(fieldTypes, 0, newFieldTypes, 0, fieldTypes.length);
    newFieldTypes[fieldTypes.length] = fieldType;
    fieldNames = newFieldNames;
    fieldTypes = newFieldTypes;
  }

  public void addColumns(String[] fieldNames, DataWorkflowFieldType[] fieldTypes) {
    if (ArrayUtils.isNotEmpty(fieldNames) && ArrayUtils.isNotEmpty(fieldTypes)) {
      if (fieldNames.length != fieldTypes.length) {
        throw new DataWorkflowException("fieldNames length is not equals fieldTypes length");
      }
    }
    if (ArrayUtils.isEmpty(this.fieldNames) && ArrayUtils.isEmpty(this.fieldTypes)) {
      this.fieldNames = fieldNames;
      this.fieldTypes = fieldTypes;
      return;
    }
    for (String fieldName : fieldNames) {
      if (ArrayUtils.contains(this.fieldNames, fieldName)) {
        throw new DataWorkflowException(String.format("Duplicate field [%s]", fieldName));
      }
    }
    int newFieldNameLength = this.fieldNames.length + fieldNames.length;
    String[] newFieldNames = new String[newFieldNameLength];
    System.arraycopy(this.fieldNames, 0, newFieldNames, 0, this.fieldNames.length);
    System.arraycopy(fieldNames, 0, newFieldNames, this.fieldNames.length, fieldNames.length);

    int newFieldType = this.fieldTypes.length + fieldTypes.length;
    DataWorkflowFieldType[] newFieldTypes = new DataWorkflowFieldType[newFieldType];
    System.arraycopy(this.fieldTypes, 0, newFieldTypes, 0, this.fieldTypes.length);
    System.arraycopy(fieldTypes, 0, newFieldTypes, this.fieldTypes.length, fieldTypes.length);

    this.fieldNames = newFieldNames;
    this.fieldTypes = newFieldTypes;
  }

  @Override
  public Object[] pollNext() {
    if (CollectionUtils.isEmpty(values)) {
      return null;
    }

    return values.poll();
  }

  public RowRecord pollNextRecord() {
    if (CollectionUtils.isEmpty(values)) {
      return null;
    }
    return SourceRowRecord.builder()
        .fieldNames(fieldNames)
        .fieldTypes(fieldTypes)
        .values(values.poll())
        .build();
  }

  @Override
  public int size() {
    return size;
  }
}
