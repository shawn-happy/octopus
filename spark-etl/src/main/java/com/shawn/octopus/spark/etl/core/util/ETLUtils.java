package com.shawn.octopus.spark.etl.core.util;

import com.shawn.octopus.spark.etl.core.common.ColumnDesc;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ETLUtils {

  public static StructType columnDescToSchema(List<ColumnDesc> schemas) {
    if (CollectionUtils.isEmpty(schemas)) {
      return null;
    }
    StructField[] structFields = new StructField[schemas.size()];
    for (int i = 0; i < schemas.size(); i++) {
      ColumnDesc columnDesc = schemas.get(i);
      structFields[i] =
          new StructField(
              columnDesc.getName(),
              DataTypes.StringType,
              columnDesc.isNullable(),
              Metadata.empty());
    }
    return new StructType(structFields);
  }
}
