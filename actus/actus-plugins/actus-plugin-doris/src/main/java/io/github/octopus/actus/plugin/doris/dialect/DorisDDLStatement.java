package io.github.octopus.actus.plugin.doris.dialect;

import io.github.octopus.actus.core.StringPool;
import io.github.octopus.actus.core.exception.SqlException;
import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.core.model.schema.AggregateAlgo;
import io.github.octopus.actus.core.model.schema.AggregateModelDefinition;
import io.github.octopus.actus.core.model.schema.ColumnDefinition;
import io.github.octopus.actus.core.model.schema.DatabaseDefinition;
import io.github.octopus.actus.core.model.schema.FieldType;
import io.github.octopus.actus.core.model.schema.IndexAlgo;
import io.github.octopus.actus.core.model.schema.IndexDefinition;
import io.github.octopus.actus.core.model.schema.PartitionDefinition;
import io.github.octopus.actus.core.model.schema.PartitionDefinition.ListPartitionDef;
import io.github.octopus.actus.core.model.schema.PartitionDefinition.RangePartitionDef;
import io.github.octopus.actus.core.model.schema.TableDefinition;
import io.github.octopus.actus.core.model.schema.TablePath;
import io.github.octopus.actus.core.model.schema.TabletDefinition;
import io.github.octopus.actus.plugin.api.dialect.DDLStatement;
import io.github.octopus.actus.plugin.api.dialect.DialectRegistry;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.doris.model.DorisIndexAlgo;
import io.github.octopus.actus.plugin.doris.model.DorisPartitionAlgo;
import io.github.octopus.actus.plugin.doris.model.DorisPartitionOperator;
import io.github.octopus.actus.plugin.doris.model.DorisTabletArgo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class DorisDDLStatement implements DDLStatement {

  private static final DorisDDLStatement DDL_STATEMENT = new DorisDDLStatement();

  private DorisDDLStatement() {}

  public static DDLStatement getDDLStatement() {
    return DDL_STATEMENT;
  }

  @Override
  public String getCreateDatabaseSql(DatabaseDefinition databaseDefinition) {
    return String.format("CREATE DATABASE IF NOT EXISTS %s", databaseDefinition.getDatabase());
  }

  @Override
  public String getDropDatabaseSql(String database) {
    return String.format("DROP DATABASE IF EXISTS %s", database);
  }

  @Override
  public String getCreateTableSql(TableDefinition tableDefinition) {
    TablePath tablePath = tableDefinition.getTablePath();
    List<ColumnDefinition> columns = tableDefinition.getColumns();
    List<IndexDefinition> indices = tableDefinition.getIndices();
    AggregateModelDefinition aggregateModel = tableDefinition.getAggregateModel();
    PartitionDefinition partition = tableDefinition.getPartition();
    TabletDefinition tablet = tableDefinition.getTablet();
    List<String> createTableSql = new ArrayList<>();
    String columnIdentifySql = buildColumnsIdentifySql(columns, indices);
    String aggregateModelSql = buildAggregateModelSql(aggregateModel);
    String partitionSql = buildPartitionSql(partition);
    String tabletSql = buildTabletSql(tablet);
    String propertiesSql = buildPropertiesSql(tableDefinition.getOptions());
    createTableSql.add(
        String.format("CREATE TABLE IF NOT EXISTS %s (", tableIdentifier(tablePath)));
    createTableSql.add(columnIdentifySql);
    createTableSql.add(") ENGINE = OLAP");
    if (StringUtils.isNotBlank(aggregateModelSql)) {
      createTableSql.add(aggregateModelSql);
    }
    if (StringUtils.isNotBlank(tableDefinition.getComment())) {
      createTableSql.add(
          "COMMENT \""
              + tableDefinition.getComment().replace("'", "''").replace("\\", "\\\\")
              + "\"");
    }
    if (StringUtils.isNotBlank(partitionSql)) {
      createTableSql.add(partitionSql);
    }
    if (StringUtils.isNotBlank(tabletSql)) {
      createTableSql.add(tabletSql);
    }
    if (StringUtils.isNotBlank(propertiesSql)) {
      createTableSql.add(propertiesSql);
    }
    return String.join(" \n", createTableSql);
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
        "ALTER TABLE %s MODIFY COMMENT = \"%s\"",
        tableIdentifier(tablePath), comment.replace("'", "''").replace("\\", "\\\\"));
  }

  @Override
  public String getModifyTableCommentSql(TablePath tablePath, String comment) {
    return String.format(
        "ALTER TABLE %s MODIFY COMMENT = \"%s\"",
        tableIdentifier(tablePath), comment.replace("'", "''").replace("\\", "\\\\"));
  }

  @Override
  public String getDropTableCommentSql(TablePath tablePath) {
    return String.format(
        "ALTER TABLE %s MODIFY COMMENT = \"%s\"",
        tableIdentifier(tablePath), StringPool.BLANK_COMMENT);
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
    return DialectRegistry.getDialect(DatabaseIdentifier.DORIS);
  }

  private String buildColumnsIdentifySql(
      List<ColumnDefinition> columnDefinitions, List<IndexDefinition> indexDefinitions) {
    List<String> columnsIdentifySql = new ArrayList<>();
    columnDefinitions.forEach(
        col -> columnsIdentifySql.add(String.format("\t%s", buildColumnSql(col))));
    if (CollectionUtils.isNotEmpty(indexDefinitions)) {
      indexDefinitions.forEach(
          index -> columnsIdentifySql.add(String.format("\t%s", buildIndexSql(index))));
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
      type = String.format("%s(%d, %d)", type, precision, scale);
    } else if (precision != null) {
      type = String.format("%s(%d)", type, precision);
    }
    columnSql.add(type);

    AggregateAlgo aggregateAlgo = columnDefinition.getAggregateAlgo();
    if (aggregateAlgo != null) {
      columnSql.add(aggregateAlgo.getAlgo());
    }
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
    DorisIndexAlgo indexAlgo = (DorisIndexAlgo) indexDefinition.getIndexAlgo();
    if (indexAlgo != null) {
      indexSql.add("USING " + indexAlgo.getAlgo());
    }
    String comment = indexDefinition.getComment();
    if (StringUtils.isNotBlank(comment)) {
      indexSql.add("COMMENT '" + comment.replace("'", "''").replace("\\", "\\\\") + "'");
    }
    return String.join(" ", indexSql);
  }

  private String buildAggregateModelSql(AggregateModelDefinition aggregateModel) {
    if (aggregateModel == null) {
      return null;
    }
    String type = aggregateModel.getType().getType();
    String columns =
        aggregateModel
            .getColumns()
            .stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", "));
    return String.format("%s (%s)", type, columns);
  }

  private String buildPartitionSql(PartitionDefinition partitionDefinition) {
    if (Objects.isNull(partitionDefinition)) {
      return null;
    }
    List<String> partitionSql = new ArrayList<>();
    DorisPartitionAlgo partitionAlgo = (DorisPartitionAlgo) partitionDefinition.getPartitionAlgo();
    String columns =
        partitionDefinition
            .getColumns()
            .stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", "));
    partitionSql.add("PARTITION BY " + partitionAlgo.getAlgo() + "(" + columns + ")");
    partitionSql.add("(");
    switch (partitionAlgo) {
      case RANGE:
        List<RangePartitionDef> rangePartitions = partitionDefinition.getRangePartitions();
        List<String> rangePartitionSql = new ArrayList<>();
        for (RangePartitionDef rangePartition : rangePartitions) {
          String name = rangePartition.getName();
          DorisPartitionOperator partitionOperator =
              (DorisPartitionOperator) rangePartition.getPartitionOperator();
          switch (partitionOperator) {
            case LessThan:
              String maxValues = "MAXVALUE";
              if (ArrayUtils.isNotEmpty(rangePartition.getMaxValues())) {
                if (partitionDefinition.getColumns().size()
                    != rangePartition.getMaxValues().length) {
                  throw new SqlException(
                      String.format(
                          "Partition columns size [%d] is not match maxValues length [%d]",
                          partitionDefinition.getColumns().size(),
                          rangePartition.getMaxValues().length));
                }
                maxValues =
                    Arrays.stream(rangePartition.getMaxValues())
                        .map(
                            v ->
                                StringUtils.isNumeric(v.toString())
                                    ? v.toString()
                                    : String.format("\"%s\"", v))
                        .collect(Collectors.joining(", "));
              }
              rangePartitionSql.add(
                  String.format("PARTITION %s VALUES LESS THAN (%s)", name, maxValues));
              break;
            case FIXED_RANGE:
              Object[] lefts = rangePartition.getLefts();
              Object[] rights = rangePartition.getRights();
              if (ArrayUtils.isEmpty(lefts) || ArrayUtils.isEmpty(rights)) {
                throw new SqlException("Fixed Range values cannot be empty");
              }
              if (lefts.length != rights.length
                  && partitionDefinition.getColumns().size() != lefts.length) {
                throw new SqlException(
                    String.format(
                        "Fixed Range left values length [%d], right values length [%d] and columns size [%d] mismatch",
                        lefts.length, rights.length, partitionDefinition.getColumns().size()));
              }
              String leftValues =
                  Arrays.stream(lefts)
                      .map(
                          v ->
                              StringUtils.isNumeric(v.toString())
                                  ? v.toString()
                                  : String.format("\"%s\"", v))
                      .collect(Collectors.joining(", "));
              String rightValues =
                  Arrays.stream(rights)
                      .map(
                          v ->
                              StringUtils.isNumeric(v.toString())
                                  ? v.toString()
                                  : String.format("\"%s\"", v))
                      .collect(Collectors.joining(", "));
              rangePartitionSql.add(
                  String.format("PARTITION %s VALUES [(%s), (%s)]", name, leftValues, rightValues));
              break;
            case DATE_MULTI_RANGE:
              if (partitionDefinition.getColumns().size() != 1) {
                throw new SqlException(
                    "partition column size in multi partition clause must be one");
              }
              Object[] dateStart = rangePartition.getLefts();
              Object[] dateEnd = rangePartition.getRights();
              if (ArrayUtils.isEmpty(dateStart) || ArrayUtils.isEmpty(dateEnd)) {
                throw new SqlException("Date Multi Range values cannot be empty");
              }
              if (dateStart.length != 1 && dateEnd.length != 1) {
                throw new SqlException(
                    String.format(
                        "partition column size in multi partition clause must be one, but start column size is: %d, and end column size is: %d",
                        dateStart.length, dateEnd.length));
              }
              rangePartitionSql.add(
                  String.format(
                      "FROM (%s) TO (%s) INTERVAL %s %s",
                      StringUtils.isNumeric(dateStart[0].toString())
                          ? dateStart[0].toString()
                          : String.format("\"%s\"", dateStart[0]),
                      StringUtils.isNumeric(dateEnd[0].toString())
                          ? dateEnd[0].toString()
                          : String.format("\"%s\"", dateEnd[0]),
                      rangePartition.getInterval(),
                      rangePartition.getIntervalType().name()));
              break;
            case NUMERIC_MULTI_RANGE:
              if (partitionDefinition.getColumns().size() != 1) {
                throw new SqlException(
                    "partition column size in multi partition clause must be one");
              }
              Object[] numStart = rangePartition.getLefts();
              Object[] numEnd = rangePartition.getRights();
              if (ArrayUtils.isEmpty(numStart) || ArrayUtils.isEmpty(numEnd)) {
                throw new SqlException("Numeric Multi Range values cannot be empty");
              }
              if (numStart.length != 1 && numEnd.length != 1) {
                throw new SqlException(
                    String.format(
                        "partition column size in multi partition clause must be one, but start column size is: %d, and end column size is: %d",
                        numStart.length, numEnd.length));
              }
              rangePartitionSql.add(
                  String.format(
                      "FROM (%s) TO (%s) INTERVAL %s",
                      numStart[0], numEnd[0], rangePartition.getInterval()));
              break;
            default:
              throw new SqlException("unsupported partition operator");
          }
        }
        partitionSql.add(String.join(",\n", rangePartitionSql));
        break;
      case LIST:
        List<ListPartitionDef> listPartitionDefs = partitionDefinition.getListPartitionDefs();
        List<String> listPartitionSql = new ArrayList<>();
        for (ListPartitionDef listPartitionDef : listPartitionDefs) {
          String name = listPartitionDef.getName();
          List<Object[]> values = listPartitionDef.getValues();
          if (CollectionUtils.isEmpty(values)) {
            throw new SqlException("List partition value cannot be empty");
          }

          List<String> listValues = new ArrayList<>();
          for (Object[] colValues : values) {
            if (ArrayUtils.isEmpty(colValues)) {
              throw new SqlException("List partition value cannot be empty");
            }
            if (colValues.length != partitionDefinition.getColumns().size()) {
              throw new SqlException(
                  String.format(
                      "List partition column size %d is not match value length: %d",
                      partitionDefinition.getColumns().size(), colValues.length));
            }
            // "Beijing"
            // (1, "Beijing")
            String listValue =
                Arrays.stream(colValues)
                    .map(
                        v ->
                            StringUtils.isNumeric(v.toString())
                                ? v.toString()
                                : String.format("\"%s\"", v))
                    .collect(Collectors.joining(", "));
            if (colValues.length > 1) {
              listValue = String.format("(%s)", listValue);
            }
            listValues.add(listValue);
          }
          listPartitionSql.add(
              String.format("PARTITION %s VALUES IN (%s)", name, String.join(", ", listValues)));
        }
        partitionSql.add(String.join(",\n", listPartitionSql));
        break;
      default:
        throw new SqlException("unsupported partition algo");
    }
    partitionSql.add(")");
    return String.join("\n", partitionSql);
  }

  private String buildTabletSql(TabletDefinition tabletDefinition) {
    if (tabletDefinition == null) {
      return "DISTRIBUTED BY RANDOM";
    }
    DorisTabletArgo algo = (DorisTabletArgo) tabletDefinition.getAlgo();
    if (algo != DorisTabletArgo.Hash) {
      throw new SqlException("unsupported algo");
    }
    List<String> columns = tabletDefinition.getColumns();
    return String.format(
        "DISTRIBUTED BY HASH (%s) BUCKETS %d",
        columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")),
        tabletDefinition.getNum());
  }

  private String buildPropertiesSql(Map<String, String> properties) {
    if (MapUtils.isEmpty(properties)) {
      return null;
    }
    String propertiesSql =
        properties
            .entrySet()
            .stream()
            .map(entry -> String.format("\"%s\" = \"%s\"", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(",\n"));
    return String.format("PROPERTIES (%s)", propertiesSql);
  }
}
