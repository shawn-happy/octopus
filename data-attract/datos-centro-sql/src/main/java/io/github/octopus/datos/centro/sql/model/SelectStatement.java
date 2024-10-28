package io.github.octopus.datos.centro.sql.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SelectStatement {

  private List<ColumnAlias> columnAliases;
  private TableAlias tableAlias;
  private WhereExpression where;
  // TODO 暂不提供Join，With等嵌套查询

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ColumnAlias {
    private String column;
    private String alias;
    // TODO 暂不提供表达式
    private String selectExpr;

    public String toSQL() {
      if (StringUtils.isNotBlank(alias)) {
        return String.format("%s AS %s", column, alias);
      }
      return column;
    }
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class TableAlias {
    private String database;
    private String table;
    private String alias;

    public String toSQL() {
      if (StringUtils.isNotBlank(database)) {
        if (StringUtils.isNotBlank(alias)) {
          return String.format("%s.%s AS %s", database, table, alias);
        }
        return String.format("%s.%s", database, table);
      } else {
        if (StringUtils.isNotBlank(alias)) {
          return String.format("%s AS %s", table, alias);
        }
        return table;
      }
    }
  }

  public String toSQL() {
    StringBuilder sqlBuilder = toCommonSQL();
    if (ObjectUtils.isNotEmpty(where)) {
      sqlBuilder.append(" WHERE ");
      sqlBuilder.append(where.toSQL());
    }
    return sqlBuilder.toString();
  }

  public String toSQLValue() {
    StringBuilder sqlBuilder = toCommonSQL();
    if (ObjectUtils.isNotEmpty(where)) {
      sqlBuilder.append(" WHERE ");
      sqlBuilder.append(where.toSQLValue());
    }
    return sqlBuilder.toString();
  }

  private StringBuilder toCommonSQL() {
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");
    if (CollectionUtils.isEmpty(columnAliases)) {
      sqlBuilder.append(" * ");
    } else {
      for (int i = 0; i < columnAliases.size(); i++) {
        ColumnAlias columnAlias = columnAliases.get(i);
        sqlBuilder.append(columnAlias.toSQL());
        if (i != columnAliases.size() - 1) {
          sqlBuilder.append(", ");
        }
      }
    }
    sqlBuilder.append(" FROM ").append(tableAlias.toSQL());
    return sqlBuilder;
  }
}
