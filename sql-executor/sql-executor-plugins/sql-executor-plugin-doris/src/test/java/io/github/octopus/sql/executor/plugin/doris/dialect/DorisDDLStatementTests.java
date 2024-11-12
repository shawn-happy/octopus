package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.IntervalType;
import io.github.octopus.sql.executor.core.model.schema.PartitionDefinition;
import io.github.octopus.sql.executor.core.model.schema.PartitionDefinition.RangePartitionDef;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.core.model.schema.TabletDefinition;
import io.github.octopus.sql.executor.plugin.doris.model.DorisFieldType;
import io.github.octopus.sql.executor.plugin.doris.model.DorisPartitionAlgo;
import io.github.octopus.sql.executor.plugin.doris.model.DorisPartitionOperator;
import io.github.octopus.sql.executor.plugin.doris.model.DorisTabletArgo;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DorisDDLStatementTests {

  @Test
  public void testCreateTableSql() {
    TableDefinition tableDefinition =
        TableDefinition.builder()
            .table("test_create_table")
            .database("test_shawn")
            .columns(
                Arrays.asList(
                    ColumnDefinition.builder()
                        .column("k1")
                        .fieldType(DorisFieldType.TINYINT)
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k2")
                        .fieldType(DorisFieldType.DATE)
                        .comment("create time")
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k3")
                        .fieldType(DorisFieldType.DECIMAL)
                        .precision(10)
                        .scale(2)
                        .build()))
            .comment("My Fist Table")
            .build();
    String createTableSql = DorisDDLStatement.getDDLStatement().getCreateTableSql(tableDefinition);
    String sql =
        "CREATE TABLE IF NOT EXISTS `test_shawn`.`test_create_table` (\n"
            + "\t`k1` TINYINT NOT NULL,\n"
            + "\t`k2` DATE NOT NULL COMMENT 'create time',\n"
            + "\t`k3` DECIMAL(10, 2) NOT NULL\n"
            + ") ENGINE = OLAP\n"
            + "COMMENT \"My Fist Table\"\n"
            + "DISTRIBUTED BY RANDOM";
    Assertions.assertEquals(sql, createTableSql);
  }

  @Test
  public void testCreateTablePartitionSql() {
    TableDefinition tableDefinition =
        TableDefinition.builder()
            .table("test_create_table")
            .database("test_shawn")
            .columns(
                Arrays.asList(
                    ColumnDefinition.builder()
                        .column("k1")
                        .fieldType(DorisFieldType.TINYINT)
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k2")
                        .fieldType(DorisFieldType.DATE)
                        .comment("create time")
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k3")
                        .fieldType(DorisFieldType.DECIMAL)
                        .precision(10)
                        .scale(2)
                        .build()))
            .partition(
                PartitionDefinition.builder()
                    .columns(Collections.singletonList("k2"))
                    .rangePartitions(
                        List.of(
                            RangePartitionDef.builder()
                                .name("p1")
                                .partitionOperator(DorisPartitionOperator.DATE_MULTI_RANGE)
                                .interval(1)
                                .intervalType(IntervalType.YEAR)
                                .lefts(new Object[] {"2000-01-01"})
                                .rights(new Object[] {"2022-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p2")
                                .partitionOperator(DorisPartitionOperator.DATE_MULTI_RANGE)
                                .interval(1)
                                .intervalType(IntervalType.MONTH)
                                .lefts(new Object[] {"2022-01-01"})
                                .rights(new Object[] {"2023-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p3")
                                .partitionOperator(DorisPartitionOperator.DATE_MULTI_RANGE)
                                .interval(1)
                                .intervalType(IntervalType.WEEK)
                                .lefts(new Object[] {"2023-01-01"})
                                .rights(new Object[] {"2024-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p4")
                                .partitionOperator(DorisPartitionOperator.DATE_MULTI_RANGE)
                                .interval(1)
                                .intervalType(IntervalType.DAY)
                                .lefts(new Object[] {"2024-01-01"})
                                .rights(new Object[] {"2025-01-01"})
                                .build()))
                    .partitionAlgo(DorisPartitionAlgo.RANGE)
                    .build())
            .tablet(
                TabletDefinition.builder()
                    .columns(Collections.singletonList("k1"))
                    .num(10)
                    .algo(DorisTabletArgo.Hash)
                    .build())
            .comment("My Fist Table")
            .build();
    String createTableSql = DorisDDLStatement.getDDLStatement().getCreateTableSql(tableDefinition);
    String sql =
        "CREATE TABLE IF NOT EXISTS `test_shawn`.`test_create_table` ( \n"
            + "\t`k1` TINYINT NOT NULL, \n"
            + "\t`k2` DATE NOT NULL COMMENT 'create time', \n"
            + "\t`k3` DECIMAL(10, 2) NOT NULL \n"
            + ") ENGINE = OLAP \n"
            + "COMMENT \"My Fist Table\" \n"
            + "PARTITION BY RANGE(`k2`)\n"
            + "(\n"
            + "FROM (\"2000-01-01\") TO (\"2022-01-01\") INTERVAL 1 YEAR,\n"
            + "FROM (\"2022-01-01\") TO (\"2023-01-01\") INTERVAL 1 MONTH,\n"
            + "FROM (\"2023-01-01\") TO (\"2024-01-01\") INTERVAL 1 WEEK,\n"
            + "FROM (\"2024-01-01\") TO (\"2025-01-01\") INTERVAL 1 DAY\n"
            + ") \n"
            + "DISTRIBUTED BY HASH (`k1`) BUCKETS 10";
    Assertions.assertEquals(sql, createTableSql);
  }

  @Test
  public void testCreateTablePartitionLessThanSql() {
    TableDefinition tableDefinition =
        TableDefinition.builder()
            .table("test_create_table")
            .database("test_shawn")
            .columns(
                Arrays.asList(
                    ColumnDefinition.builder()
                        .column("k1")
                        .fieldType(DorisFieldType.TINYINT)
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k2")
                        .fieldType(DorisFieldType.DATE)
                        .comment("create time")
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k3")
                        .fieldType(DorisFieldType.DECIMAL)
                        .precision(10)
                        .scale(2)
                        .build()))
            .partition(
                PartitionDefinition.builder()
                    .columns(Collections.singletonList("k2"))
                    .rangePartitions(
                        List.of(
                            RangePartitionDef.builder()
                                .name("p1")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .maxValues(new Object[] {"2022-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p2")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .maxValues(new Object[] {"2023-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p3")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .maxValues(new Object[] {"2024-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p4")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .build()))
                    .partitionAlgo(DorisPartitionAlgo.RANGE)
                    .build())
            .tablet(
                TabletDefinition.builder()
                    .columns(Collections.singletonList("k1"))
                    .num(10)
                    .algo(DorisTabletArgo.Hash)
                    .build())
            .comment("My Fist Table")
            .build();
    String createTableSql = DorisDDLStatement.getDDLStatement().getCreateTableSql(tableDefinition);
    String sql =
        "CREATE TABLE IF NOT EXISTS `test_shawn`.`test_create_table` ( \n"
            + "\t`k1` TINYINT NOT NULL, \n"
            + "\t`k2` DATE NOT NULL COMMENT 'create time', \n"
            + "\t`k3` DECIMAL(10, 2) NOT NULL \n"
            + ") ENGINE = OLAP \n"
            + "COMMENT \"My Fist Table\" \n"
            + "PARTITION BY RANGE(`k2`)\n"
            + "(\n"
            + "PARTITION p1 VALUES LESS THAN (\"2022-01-01\"),\n"
            + "PARTITION p2 VALUES LESS THAN (\"2023-01-01\"),\n"
            + "PARTITION p3 VALUES LESS THAN (\"2024-01-01\"),\n"
            + "PARTITION p4 VALUES LESS THAN (MAXVALUE)\n"
            + ") \n"
            + "DISTRIBUTED BY HASH (`k1`) BUCKETS 10";
    Assertions.assertEquals(sql, createTableSql);
  }

  @Test
  public void testCreateTablePartitionFixedRangeSql() {
    TableDefinition tableDefinition =
        TableDefinition.builder()
            .table("test_create_table")
            .database("test_shawn")
            .columns(
                Arrays.asList(
                    ColumnDefinition.builder()
                        .column("k1")
                        .fieldType(DorisFieldType.TINYINT)
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k2")
                        .fieldType(DorisFieldType.DATE)
                        .comment("create time")
                        .nullable(false)
                        .build(),
                    ColumnDefinition.builder()
                        .column("k3")
                        .fieldType(DorisFieldType.DECIMAL)
                        .precision(10)
                        .scale(2)
                        .build()))
            .partition(
                PartitionDefinition.builder()
                    .columns(Collections.singletonList("k2"))
                    .rangePartitions(
                        List.of(
                            RangePartitionDef.builder()
                                .name("p1")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .maxValues(new Object[] {"2022-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p2")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .maxValues(new Object[] {"2023-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p3")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .maxValues(new Object[] {"2024-01-01"})
                                .build(),
                            RangePartitionDef.builder()
                                .name("p4")
                                .partitionOperator(DorisPartitionOperator.LessThan)
                                .build()))
                    .partitionAlgo(DorisPartitionAlgo.RANGE)
                    .build())
            .tablet(
                TabletDefinition.builder()
                    .columns(Collections.singletonList("k1"))
                    .num(10)
                    .algo(DorisTabletArgo.Hash)
                    .build())
            .comment("My Fist Table")
            .build();
    String createTableSql = DorisDDLStatement.getDDLStatement().getCreateTableSql(tableDefinition);
    String sql =
        "CREATE TABLE IF NOT EXISTS `test_shawn`.`test_create_table` ( \n"
            + "\t`k1` TINYINT NOT NULL, \n"
            + "\t`k2` DATE NOT NULL COMMENT 'create time', \n"
            + "\t`k3` DECIMAL(10, 2) NOT NULL \n"
            + ") ENGINE = OLAP \n"
            + "COMMENT \"My Fist Table\" \n"
            + "PARTITION BY RANGE(`k2`)\n"
            + "(\n"
            + "PARTITION p1 VALUES LESS THAN (\"2022-01-01\"),\n"
            + "PARTITION p2 VALUES LESS THAN (\"2023-01-01\"),\n"
            + "PARTITION p3 VALUES LESS THAN (\"2024-01-01\"),\n"
            + "PARTITION p4 VALUES LESS THAN (MAXVALUE)\n"
            + ") \n"
            + "DISTRIBUTED BY HASH (`k1`) BUCKETS 10";
    Assertions.assertEquals(sql, createTableSql);
  }
}
