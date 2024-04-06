package io.github.shawn.octopus.fluxus.engine.model.table;

import io.github.shawn.octopus.fluxus.api.config.Column;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DataWorkflowFieldTypeParse;
import io.github.shawn.octopus.fluxus.engine.model.type.DecimalFieldType;
import java.util.ArrayList;
import java.util.List;

public abstract class SchemaUtils {

  public static List<Schema> getSchemas(List<Column> columns) {
    List<Schema> schemas = new ArrayList<>();
    for (Column column : columns) {
      DataWorkflowFieldType dataWorkflowFieldType =
          DataWorkflowFieldTypeParse.parseDataType(column.getType());
      Integer precision = column.getLength();
      Integer scale = null;
      if (dataWorkflowFieldType instanceof DecimalFieldType) {
        DecimalFieldType decimalFieldType = (DecimalFieldType) dataWorkflowFieldType;
        precision = decimalFieldType.getPrecision();
        scale = decimalFieldType.getScale();
      }
      schemas.add(
          Schema.builder()
              .fieldName(column.getName())
              .fieldType(DataWorkflowFieldTypeParse.parseDataType(column.getType()))
              .nullable(column.isNullable())
              .defaultValue(column.getDefaultValue())
              .comment(column.getComment())
              .precision(precision)
              .scale(scale)
              .build());
    }
    return schemas;
  }
}
