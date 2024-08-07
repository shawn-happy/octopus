package io.github.octopus.datos.centro.sql.model;

import com.alibaba.druid.DbType;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DorisFieldType;
import io.github.octopus.datos.centro.sql.model.dialect.mysql.MySQLFieldType;

public interface FieldType {
  /** 数据类型 */
  String getDataType();

  /** 描述 */
  String getDescription();

  String toString();

  /** 是否是数字类型 */
  boolean isNumeric();

  /** 是否是字符串类型 */
  boolean isString();

  /** 是否是时间日期类型 */
  boolean isDateTime();

  static FieldType of(DbType dbType, String fieldType) {
    switch (dbType) {
      case starrocks:
        return DorisFieldType.of(fieldType);
      case mysql:
        return MySQLFieldType.of(fieldType);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
