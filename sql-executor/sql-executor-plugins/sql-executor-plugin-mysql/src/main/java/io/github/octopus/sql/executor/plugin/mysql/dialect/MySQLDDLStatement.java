package io.github.octopus.sql.executor.plugin.mysql.dialect;

import io.github.octopus.sql.executor.core.StringPool;
import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintType;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.IndexAlgo;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import io.github.octopus.sql.executor.plugin.api.dialect.DDLStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class MySQLDDLStatement implements DDLStatement {

  private static final MySQLDDLStatement DDL_STATEMENT = new MySQLDDLStatement();

  private MySQLDDLStatement() {}

  public static DDLStatement getDDLStatement() {
    return DDL_STATEMENT;
  }

  @Override
  public String getCreateTableSql(TableDefinition tableDefinition) {
    TablePath tablePath = TablePath.of(tableDefinition.getDatabase(), tableDefinition.getTable());
    String tableName = tableIdentifier(tablePath);
    List<ColumnDefinition> columns = tableDefinition.getColumns();
    List<IndexDefinition> indices = tableDefinition.getIndices();
    List<ConstraintDefinition> constraints = tableDefinition.getConstraints();
    String columnsIdentifySql = buildColumnsIdentifySql(columns, indices, constraints);
    String comment = tableDefinition.getComment();
    // TODO: engine, charset , collate
    List<String> sql = new ArrayList<>();
    sql.add(String.format("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", tableName, columnsIdentifySql));
    sql.add("engine = INNODB CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci");

    if (comment != null) {
      sql.add("COMMENT = '" + comment.replace("'", "''").replace("\\", "\\\\") + "'");
    }
    return String.join(" ", sql);
  }

  @Override
  public String getRenameTableSql(TablePath oldTablePath, String newTableName) {
    return String.format(
        "ALTER TABLE %s RENAME %s",
        tableIdentifier(oldTablePath),
        tableIdentifier(TablePath.of(oldTablePath.getDatabaseName(), newTableName)));
  }

  @Override
  public String getAddTableCommentSql(TablePath tablePath, String comment) {
    return String.format(
        "ALTER TABLE %s COMMENT = '%s'",
        tableIdentifier(tablePath), comment.replace("'", "''").replace("\\", "\\\\"));
  }

  @Override
  public String getModifyTableCommentSql(TablePath tablePath, String comment) {
    return String.format(
        "ALTER TABLE %s COMMENT = '%s'",
        tableIdentifier(tablePath), comment.replace("'", "''").replace("\\", "\\\\"));
  }

  @Override
  public String getDropTableCommentSql(TablePath tablePath) {
    return String.format(
        "ALTER TABLE %s COMMENT = '%s'", tableIdentifier(tablePath), StringPool.BLANK_COMMENT);
  }

  @Override
  public String getAddColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    return String.format(
        "ALTER TABLE %s ADD COLUMN %s",
        tableIdentifier(tablePath), buildColumnSql(columnDefinition));
  }

  @Override
  public String getRenameColumnSql(TablePath tablePath, String oldColumn, String newColumn) {
    return String.format(
        "ALTER TABLE %s RENAME COLUMN %s TO %s",
        tableIdentifier(tablePath), quoteIdentifier(oldColumn), quoteIdentifier(newColumn));
  }

  @Override
  public String getModifyColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    return String.format(
        "ALTER TABLE %s MODIFY COLUMN %s",
        tableIdentifier(tablePath), buildColumnSql(columnDefinition));
  }

  @Override
  public String getDropColumnSql(TablePath tablePath, String column) {
    return String.format("ALTER TABLE %s DROP COLUMN %s", tableIdentifier(tablePath), column);
  }

  @Override
  public String getAddColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment) {
    columnDefinition.setComment(comment);
    return getModifyColumnSql(tablePath, columnDefinition);
  }

  @Override
  public String getModifyColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment) {
    columnDefinition.setComment(comment);
    return getModifyColumnSql(tablePath, columnDefinition);
  }

  @Override
  public String getDropColumnCommentSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    columnDefinition.setComment(StringPool.BLANK_COMMENT);
    return getModifyColumnSql(tablePath, columnDefinition);
  }

  @Override
  public String getCreateIndexSql(TablePath tablePath, IndexDefinition indexDefinition) {
    List<String> createIndexSql = new ArrayList<>();
    createIndexSql.add("CREATE INDEX");
    String name = indexDefinition.getName();
    createIndexSql.add(quoteIdentifier(name));
    createIndexSql.add("ON");
    String table = tableIdentifier(tablePath);
    createIndexSql.add(table);
    String columns =
        indexDefinition
            .getColumns()
            .stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", "));
    createIndexSql.add(columns);
    IndexAlgo indexAlgo = indexDefinition.getIndexAlgo();
    if (indexAlgo != null) {
      createIndexSql.add("USING " + indexAlgo.getAlgo());
    }
    String comment = indexDefinition.getComment();
    if (StringUtils.isNotBlank(comment)) {
      createIndexSql.add("COMMENT '" + comment.replace("'", "''").replace("\\", "\\\\") + "'");
    }
    return String.join(" ", createIndexSql);
  }

  @Override
  public String getDropIndexSql(TablePath tablePath, String index) {
    // DROP INDEX `${index}` ON <if test="database != null">`${database}`.</if>`${table}`
    return String.format("DROP INDEX %s ON %s", quoteIdentifier(index), tableIdentifier(tablePath));
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.MYSQL);
  }

  private String buildColumnsIdentifySql(
      List<ColumnDefinition> columnDefinitions,
      List<IndexDefinition> indexDefinitions,
      List<ConstraintDefinition> constraintDefinitions) {
    List<String> columnsIdentifySql = new ArrayList<>();
    columnDefinitions.forEach(
        col -> columnsIdentifySql.add(String.format("\t%s", buildColumnSql(col))));
    if (CollectionUtils.isNotEmpty(indexDefinitions)) {
      indexDefinitions.forEach(
          index -> columnsIdentifySql.add(String.format("\t%s", buildIndexSql(index))));
    }
    if (CollectionUtils.isNotEmpty(constraintDefinitions)) {
      constraintDefinitions.forEach(
          constraint ->
              columnsIdentifySql.add(String.format("\t%s", buildConstraintSql(constraint))));
    }
    return String.join(", \n", columnsIdentifySql);
  }

  private String buildColumnSql(ColumnDefinition columnDefinition) {
    List<String> columnSql = new ArrayList<>();
    // col type(p,s) not null default <> comment '',
    String column = columnDefinition.getColumn();
    columnSql.add(quoteIdentifier(column));
    FieldType fieldType = columnDefinition.getFieldType();
    Integer precision = columnDefinition.getPrecision();
    Integer scale = columnDefinition.getScale();
    String type = fieldType.getDataType();
    if (precision != null && scale != null) {
      type = String.format("(%d, %d)", precision, scale);
    } else if (precision != null) {
      type = String.format("(%d)", precision);
    }
    columnSql.add(type);
    if (columnDefinition.isNullable()) {
      columnSql.add("NULL");
    } else {
      columnSql.add("NOT NULL");
    }
    if (columnDefinition.isAutoIncrement()) {
      columnSql.add("AUTO_INCREMENT");
    }

    //    Object defaultValue = columnDefinition.getDefaultValue();
    //    if(!Objects.isNull(defaultValue)){
    //      columnSqls.add("DEFAULT :")
    //    }
    String comment = columnDefinition.getComment();
    if (StringUtils.isNotBlank(comment)) {
      columnSql.add("COMMENT '" + comment.replace("'", "''").replace("\\", "\\\\") + "'");
    }
    return String.join(" ", columnSql);
  }

  private String buildIndexSql(IndexDefinition indexDefinition) {
    if (indexDefinition == null) {
      return "";
    }
    List<String> indexSql = new ArrayList<>();
    String name = indexDefinition.getName();
    indexSql.add(quoteIdentifier(name));
    List<String> columns = indexDefinition.getColumns();
    String indexColumns =
        String.format(
            "(%s)", columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")));
    indexSql.add(indexColumns);
    IndexAlgo indexAlgo = indexDefinition.getIndexAlgo();
    if (indexAlgo != null) {
      indexSql.add("USING " + indexAlgo.getAlgo());
    }
    String comment = indexDefinition.getComment();
    if (StringUtils.isNotBlank(comment)) {
      indexSql.add("COMMENT '" + comment.replace("'", "''").replace("\\", "\\\\") + "'");
    }
    return String.join(" ", indexSql);
  }

  private String buildConstraintSql(ConstraintDefinition constraintDefinition) {
    if (constraintDefinition == null) {
      return "";
    }
    ConstraintType constraintType = constraintDefinition.getConstraintType();
    if (constraintType == ConstraintType.FOREIGN_KEY
        || constraintType == ConstraintType.CHECK_KEY) {
      throw new UnsupportedOperationException("unsupported constraint type: " + constraintType);
    }
    String key = constraintType.getType();
    List<String> constraintSql = new ArrayList<>();
    constraintSql.add(key);
    String name = constraintDefinition.getName();
    constraintSql.add(quoteIdentifier(name));
    List<String> columns = constraintDefinition.getColumns();
    constraintSql.add(
        columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")));
    return String.join(" ", constraintSql);
  }
}
