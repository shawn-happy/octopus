package io.github.octopus.sql.executor.plugin.api.mapper;

import io.github.octopus.sql.executor.core.entity.Delete;
import io.github.octopus.sql.executor.core.entity.Insert;
import io.github.octopus.sql.executor.core.entity.Update;
import io.github.octopus.sql.executor.core.entity.Upsert;
import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.model.curd.DeleteStatement;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.expression.Expression;
import io.github.octopus.sql.executor.core.model.expression.LogicalExpression;
import io.github.octopus.sql.executor.core.model.expression.RelationExpression;
import io.github.octopus.sql.executor.core.model.op.InternalRelationalOp;
import io.github.octopus.sql.executor.core.model.op.RelationalOp;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintType;
import io.github.octopus.sql.executor.core.model.schema.ParamValue;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

public class CurdMapper {
  public static Insert toInsert(InsertStatement insertStatement) {
    if (insertStatement == null) {
      return null;
    }
    List<String> columns = insertStatement.getColumns();
    Insert.InsertBuilder builder =
        Insert.builder()
            .database(insertStatement.getDatabase())
            .table(insertStatement.getTable())
            .columns(columns);
    List<Object[]> values = insertStatement.getValues();
    if (CollectionUtils.isEmpty(values)) {
      return null;
    }

    if (values.size() == 1) {
      builder.params(toParams(values.get(0), columns));
    } else {
      List<Map<String, Object>> batchParams = new ArrayList<>();
      for (Object[] rows : values) {
        batchParams.add(toParams(rows, columns));
      }
      builder.batchParams(batchParams);
    }
    return builder.build();
  }

  public static Upsert toUpsert(TableDefinition definition, UpsertStatement upsertStatement) {
    if (upsertStatement == null) {
      return null;
    }
    List<ConstraintDefinition> constraints = definition.getConstraints();
    if (CollectionUtils.isEmpty(constraints)) {
      throw new SqlException("upsert constraint cannot be empty");
    }

    List<String> uniqueColumns =
        constraints
            .stream()
            .filter(
                constraintDefinition ->
                    constraintDefinition.getConstraintType() == ConstraintType.PRIMARY_KEY
                        || constraintDefinition.getConstraintType() == ConstraintType.UNIQUE_KEY)
            .flatMap(ukCol -> ukCol.getColumns().stream())
            .distinct()
            .collect(Collectors.toList());
    if (CollectionUtils.isEmpty(uniqueColumns)) {
      throw new SqlException("upsert need unique column");
    }
    List<ColumnDefinition> columnDefinitions = definition.getColumns();
    List<String> nonUniqueColumns =
        columnDefinitions
            .stream()
            .map(ColumnDefinition::getColumn)
            .filter(column -> !uniqueColumns.contains(column))
            .collect(Collectors.toList());

    List<String> columns = upsertStatement.getColumns();
    Upsert.UpsertBuilder upsertBuilder =
        Upsert.builder()
            .database(upsertStatement.getDatabase())
            .table(upsertStatement.getTable())
            .columns(upsertStatement.getColumns())
            .uniqueColumns(uniqueColumns)
            .nonUniqueColumns(nonUniqueColumns);
    List<Object[]> values = upsertStatement.getValues();
    if (CollectionUtils.isEmpty(values)) {
      return null;
    }
    if (values.size() == 1) {
      upsertBuilder.params(toParams(values.get(0), columns));
    }
    return upsertBuilder.build();
  }

  public static Update toUpdate(UpdateStatement updateStatement) {
    return Optional.ofNullable(updateStatement)
        .map(
            update ->
                Update.builder()
                    .database(update.getDatabase())
                    .table(update.getTable())
                    .updateParams(update.getUpdateParams().getUpdateParams())
                    .whereParams(update.getExpression().toSQL())
                    .build())
        .orElse(null);
  }

  public static Delete toDelete(DeleteStatement deleteStatement) {
    return Optional.ofNullable(deleteStatement)
        .map(
            delete ->
                Delete.builder()
                    .database(delete.getDatabase())
                    .table(delete.getTable())
                    .whereParams(delete.getExpression().toSQL())
                    .build())
        .orElse(null);
  }

  public static Map<String, Object> toParamMap(Expression where) {
    if (where == null) {
      return null;
    }
    Map<String, Object> params = new HashMap<>();
    if (where instanceof RelationExpression) {
      RelationExpression expression = ((RelationExpression) where);
      ParamValue[] pvs = expression.getParamValues();
      RelationalOp relationalOp = expression.getRelationalOp();
      for (ParamValue pv : pvs) {
        if (relationalOp == InternalRelationalOp.IN
            || relationalOp == InternalRelationalOp.NOT_IN) {
          Object[] values = pv.getValues();
          if (ArrayUtils.isEmpty(values)) {
            throw new SqlException("values cannot be null when use in/not in operator");
          }
          pv.multiParamName(relationalOp);
          params.putAll(pv.getParamIndexValue());
        } else {
          params.put(pv.getParam(), pv.getValue());
        }
      }
    } else if (where instanceof LogicalExpression) {
      List<Expression> leftExpressions = ((LogicalExpression) where).getLeftExpressions();
      if (CollectionUtils.isEmpty(leftExpressions)) {
        return params;
      }
      for (Expression leftExpression : leftExpressions) {
        Map<String, Object> leftParamMap = toParamMap(leftExpression);
        if (MapUtils.isNotEmpty(leftParamMap)) {
          params.putAll(leftParamMap);
        }
      }
      List<Expression> rightExpressions = ((LogicalExpression) where).getRightExpressions();
      if (CollectionUtils.isEmpty(rightExpressions)) {
        return params;
      }
      for (Expression rightExpression : rightExpressions) {
        Map<String, Object> rightParamMap = toParamMap(rightExpression);
        if (MapUtils.isNotEmpty(rightParamMap)) {
          params.putAll(rightParamMap);
        }
      }
    }
    return params;
  }

  private static Map<String, Object> toParams(Object[] rows, List<String> columns) {
    if (rows.length != columns.size()) {
      throw new IllegalStateException(
          String.format(
              "illegal row length, row length: [%s], columns size: [%s]",
              rows.length, columns.size()));
    }
    // 使用LinkedHashMap保证顺序与columns一致
    Map<String, Object> params = new LinkedHashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      Object row = rows[i];
      params.put(column, row);
    }
    return params;
  }
}
