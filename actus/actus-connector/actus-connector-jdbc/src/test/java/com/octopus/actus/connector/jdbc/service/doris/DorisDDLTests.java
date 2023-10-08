package com.octopus.actus.connector.jdbc.service.doris;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.octopus.actus.connector.jdbc.model.ColumnInfo;
import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.IndexInfo;
import com.octopus.actus.connector.jdbc.model.IntervalType;
import com.octopus.actus.connector.jdbc.model.PartitionInfo;
import com.octopus.actus.connector.jdbc.model.TableInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.AggregateType;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DataModelInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DistributionInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisDistributionArgo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisFieldType;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisPartitionAlgo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisPartitionOperator;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLFieldType;
import com.octopus.actus.connector.jdbc.service.DDLTestCommon;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDDLService;
import com.octopus.actus.connector.jdbc.service.impl.DataWarehouseDDLServiceImpl;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class DorisDDLTests extends DorisTestsCommon {

  private DataWarehouseDDLService dataWarehouseDDLService;
  private DDLTestCommon testCommon;

  @BeforeEach
  public void init() {
    dataWarehouseDDLService = new DataWarehouseDDLServiceImpl(properties);
    testCommon = new DDLTestCommon(url, username, password, driverClass);
    DatabaseInfo databaseInfo = DatabaseInfo.builder().name(database).build();
    dataWarehouseDDLService.createDatabase(databaseInfo);
  }

  @AfterEach
  public void destroy() {
    dataWarehouseDDLService.dropDatabase(database);
  }

  @Test
  public void testCreateDatabase() {
    assertTrue(testCommon.hasDatabase(database));
  }

  @Test
  public void testCreateDatabaseIfExists() {
    DatabaseInfo databaseInfo = DatabaseInfo.builder().name(database).build();
    assertTrue(testCommon.hasDatabase(database));
    dataWarehouseDDLService.createDatabase(databaseInfo);
    assertTrue(testCommon.hasDatabase(database));
  }

  @Test
  public void testDropDatabase() {
    assertTrue(testCommon.hasDatabase(database));
    dataWarehouseDDLService.dropDatabase(database);
    assertFalse(testCommon.hasDatabase(database));
  }

  @Test
  public void testDropDatabaseIfNotExists() {
    dataWarehouseDDLService.dropDatabase(database);
    assertFalse(testCommon.hasDatabase(database));
    dataWarehouseDDLService.dropDatabase(database);
    assertFalse(testCommon.hasDatabase(database));
  }

  @Test
  public void testCreateTableSimple() {
    String tableName = "testCreateTableSimple";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);

    assertTrue(testCommon.hasTable(database, tableName));

    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    assertTrue(testCommon.hasColumn(database, tableName, "username"));
    assertTrue(testCommon.hasColumn(database, tableName, "password"));
    assertTrue(testCommon.hasColumn(database, tableName, "create_time"));

    assertTrue(testCommon.matchColumnType(database, tableName, "id", "BIGINT"));
    assertTrue(testCommon.matchColumnType(database, tableName, "create_time", "DATETIME"));
    assertTrue(testCommon.matchColumnType(database, tableName, "username", "VARCHAR(100)"));
    assertTrue(testCommon.matchColumnType(database, tableName, "password", "VARCHAR(100)"));

    assertFalse(testCommon.columnIsPk(database, tableName, "id"));
    assertFalse(testCommon.columnIsPk(database, tableName, "create_time"));
    assertFalse(testCommon.columnIsNullable(database, tableName, "username"));
  }

  @Test
  public void testCreateTableNoDistribution() {
    String tableName = "testCreateTableNoDistribution";
    TableInfo info =
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(columns)
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithDataModel() {
    String tableName = "testCreateTableWithDataModel";
    TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .dataModelInfo(
                DataModelInfo.builder()
                    .columns(Arrays.asList("id", "username"))
                    .aggregateType(AggregateType.DUPLICATE_KEY)
                    .build())
            .build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testCreateTableWithComment() {
    String tableName = "testCreateTableWithComment";
    TableInfo info = tableInfoBuilder.name(tableName).comment(tableName).build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testCreateTableIfExists() {
    String tableName = "test";
    TableInfo info = tableInfoBuilder.name(tableName).comment(tableName).build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testCreateTableWithLessThanPartition() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.LessThan)
                    .names(Arrays.asList("p2022", "p2023", "p2024"))
                    .maxValues(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  // partition比maxValue的个数多一个
  @Test
  public void testCreateTableWithLessThanPartitionButPartitionNotMatch() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.LessThan)
                    .names(Arrays.asList("p2022", "p2023", "p2024"))
                    .maxValues(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithLessThanPartitionButPartitionNotMatch2() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.LessThan)
                    .names(Arrays.asList("p2022", "p2023"))
                    .maxValues(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  // column比maxValue的个数少一个
  @Test
  public void testCreateTableWithLessThanPartitionButColumnNotMatch() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.LessThan)
                    .names(Arrays.asList("p2022", "p2023", "p2024"))
                    .maxValues(
                        Arrays.asList(
                            new String[] {"2021-12-31 23:59:59", "2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithLessThanPartition2() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .partitionOperator(DorisPartitionOperator.LessThan)
                    .names(Arrays.asList("p2022", "p2023", "p2024"))
                    .maxValues(
                        Arrays.asList(
                            new String[] {"2021-12-31 23:59:59", "2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithLessThanPartition3() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.LessThan)
                    .maxValues(
                        Arrays.asList(
                            new String[] {"2021-12-31 23:59:59", "2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithFixedRangePartition() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.FIXED_RANGE)
                    .names(Arrays.asList("p2022", "p2023", "p2024"))
                    .lefts(
                        Arrays.asList(
                            new String[] {"2022-01-01 00:00:00"},
                            new String[] {"2023-01-01 00:00:00"},
                            new String[] {"2024-01-01 00:00:00"}))
                    .rights(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testCreateTableWithFixedRangePartition2() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.FIXED_RANGE)
                    .names(Arrays.asList("p2022", "p2023"))
                    .lefts(
                        Arrays.asList(
                            new String[] {"2022-01-01 00:00:00"},
                            new String[] {"2023-01-01 00:00:00"},
                            new String[] {"2024-01-01 00:00:00"}))
                    .rights(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithFixedRangePartition3() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.FIXED_RANGE)
                    .names(Arrays.asList("p2022", "p2023", "p2024"))
                    .lefts(
                        Arrays.asList(
                            new String[] {"2022-01-01 00:00:00", "2022-01-01 00:00:00"},
                            new String[] {"2023-01-01 00:00:00"},
                            new String[] {"2024-01-01 00:00:00"}))
                    .rights(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithFixedRangePartition4() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.FIXED_RANGE)
                    .names(Arrays.asList("p2022", "p2023", "p2024"))
                    .lefts(
                        Arrays.asList(
                            new String[] {"2022-01-01 00:00:00"},
                            new String[] {"2023-01-01 00:00:00"},
                            new String[] {"2024-01-01 00:00:00"}))
                    .rights(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59", "2022-01-01 00:00:00"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .build())
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDDLService.createTable(info));
  }

  @Test
  public void testCreateTableWithDateMultiRangePartition() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("create_time"))
                    .partitionOperator(DorisPartitionOperator.DATE_MULTI_RANGE)
                    .lefts(
                        Arrays.asList(
                            new String[] {"2022-01-01 00:00:00"},
                            new String[] {"2023-01-01 00:00:00"},
                            new String[] {"2024-01-01 00:00:00"}))
                    .rights(
                        Arrays.asList(
                            new String[] {"2022-12-31 23:59:59"},
                            new String[] {"2023-12-31 23:59:59"},
                            new String[] {"2024-12-31 23:59:59"}))
                    .interval(new int[] {1, 1, 1})
                    .intervalType(
                        new IntervalType[] {
                          IntervalType.MONTH, IntervalType.WEEK, IntervalType.DAY
                        })
                    .build())
            .build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testCreateTableWithNumericMultiRangePartition() {
    String tableName = "test";
    final TableInfo info =
        tableInfoBuilder
            .name(tableName)
            .partitionInfo(
                PartitionInfo.builder()
                    .partitionAlgo(DorisPartitionAlgo.Range)
                    .columns(Collections.singletonList("id"))
                    .partitionOperator(DorisPartitionOperator.NUMERIC_MULTI_RANGE)
                    .lefts(Arrays.asList(new Long[] {1L}, new Long[] {100L}, new Long[] {200L}))
                    .rights(Arrays.asList(new Long[] {100L}, new Long[] {200L}, new Long[] {300L}))
                    .interval(new int[] {1, 1, 1})
                    .build())
            .build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testDropTable() {
    String tableName = "testCreateTableSimple";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
    dataWarehouseDDLService.dropTable(database, tableName);
    assertFalse(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testDropTableIfNotExists() {
    String tableName = "testCreateTableSimple";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
    dataWarehouseDDLService.dropTable(database, tableName);
    assertFalse(testCommon.hasTable(database, tableName));
    dataWarehouseDDLService.dropTable(database, tableName);
    assertFalse(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testRenameTable() {
    String oldTableName = "testCreateTableSimple";
    TableInfo info = tableInfoBuilder.name(oldTableName).build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, oldTableName));
    String newTableName = "newTable";
    dataWarehouseDDLService.renameTable(database, oldTableName, newTableName);
    assertFalse(testCommon.hasTable(database, oldTableName));
    assertTrue(testCommon.hasTable(database, newTableName));
  }

  @Test
  public void testAddTableComment() {
    String tableName = "testAddTableComment";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    assertTrue(testCommon.hasTable(database, tableName));
    String comment = "添加表注释";
    dataWarehouseDDLService.addTableComment(database, tableName, comment);
  }

  @Test
  public void testModifyTableComment() {
    String initComment = "初始表注释";
    String tableName = "testAddTableComment";
    TableInfo info = tableInfoBuilder.name(tableName).comment(initComment).build();
    dataWarehouseDDLService.createTable(info);
    String newComment = "修改后的表注释";
    dataWarehouseDDLService.modifyTableComment(database, tableName, newComment);
  }

  @Test
  public void testRemoveTableComment() {
    String initComment = "初始表注释";
    String tableName = "testAddTableComment";
    TableInfo info = tableInfoBuilder.name(tableName).comment(initComment).build();
    dataWarehouseDDLService.createTable(info);
    dataWarehouseDDLService.removeTableComment(database, tableName);
  }

  @Test
  public void testAddColumn() {
    String tableName = "testAddColumn";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    String newColumn = "update_time";
    dataWarehouseDDLService.addColumn(
        database,
        tableName,
        ColumnInfo.builder()
            .name(newColumn)
            .fieldType(DorisFieldType.DateTime)
            .nullable(true)
            .build());
  }

  @Test
  public void testAddColumnWithDefaultValue() {
    String tableName = "testAddColumn";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    String newColumn = "update_time";
    dataWarehouseDDLService.addColumn(
        database,
        tableName,
        ColumnInfo.builder()
            .name(newColumn)
            .fieldType(DorisFieldType.DateTime)
            .nullable(false)
            .defaultValue(LocalDateTime.now())
            .build());
  }

  @Test
  public void testRenameColumn() {
    String tableName = "testRenameColumn";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    dataWarehouseDDLService.renameColumn(database, tableName, "create_time", "createTime");
  }

  @Test
  public void testModifyColumn() {
    String tableName = "testModifyColumn";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    dataWarehouseDDLService.modifyColumn(
        database,
        tableName,
        ColumnInfo.builder()
            .name("username")
            .fieldType(MySQLFieldType.Varchar)
            .precision(1000)
            .nullable(false)
            .build());
  }

  @Test
  public void testModifyColumnNameAndType() {
    String tableName = "testModifyColumn";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .build());
    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    assertTrue(testCommon.matchColumnType(database, tableName, "id", "bigint"));
    assertFalse(testCommon.columnHasDefaultValue(database, tableName, "id"));
    dataWarehouseDDLService.modifyColumn(
        database,
        tableName,
        ColumnInfo.builder()
            .name("id")
            .fieldType(MySQLFieldType.Varchar)
            .precision(100)
            .nullable(false)
            .defaultValue(UUID.randomUUID().toString())
            .build());
    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    assertTrue(testCommon.matchColumnType(database, tableName, "id", "Varchar(100)"));
    assertTrue(testCommon.columnHasDefaultValue(database, tableName, "id"));
  }

  @Test
  public void testDropColumn() throws Exception {
    String tableName = "testModifyColumn";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    TimeUnit.SECONDS.sleep(1);
    dataWarehouseDDLService.dropColumn(database, tableName, "username");
    TimeUnit.SECONDS.sleep(1);
    assertTrue(testCommon.alterTableDropColumnSuccess(database, tableName));
  }

  @Test
  public void testDropColumnIfNotExists() throws Exception {
    String tableName = "testModifyColumn";
    TableInfo info = tableInfoBuilder.name(tableName).build();
    dataWarehouseDDLService.createTable(info);
    TimeUnit.SECONDS.sleep(1);
    dataWarehouseDDLService.dropColumn(database, tableName, "username");
    TimeUnit.SECONDS.sleep(1);
    assertTrue(testCommon.alterTableDropColumnSuccess(database, tableName));
    Assertions.assertThrows(
        Exception.class, () -> dataWarehouseDDLService.dropColumn(database, tableName, "username"));
  }

  @Test
  public void testDropColumnThenNoColumn() {
    String tableName = "testDropColumn";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(DorisFieldType.BigInt).build()))
            .distributionInfo(
                DistributionInfo.builder()
                    .distributionAlgo(DorisDistributionArgo.Hash)
                    .columns(Collections.singletonList("id"))
                    .num(1)
                    .build())
            .build());
    Assertions.assertThrows(
        Exception.class, () -> dataWarehouseDDLService.dropColumn(database, tableName, "id"));
  }

  @Test
  public void testDropPKColumn() {
    String tableName = "testDropPKColumn";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    Assertions.assertThrows(
        Exception.class, () -> dataWarehouseDDLService.dropColumn(database, tableName, "id"));
  }

  @Test
  public void testAddColumnComment() throws Exception {
    String tableName = "testAddColumnComment";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String comment = "user name";
    TimeUnit.SECONDS.sleep(1);
    dataWarehouseDDLService.addColumnComment(
        database,
        tableName,
        ColumnInfo.builder()
            .name("username")
            .fieldType(DorisFieldType.Varchar)
            .precision(50)
            .comment(comment)
            .build());
  }

  @Test
  public void testAddColumnCommentIfExists() throws Exception {
    String tableName = "testAddColumnComment";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String comment = "user name";
    TimeUnit.SECONDS.sleep(1);
    dataWarehouseDDLService.addColumnComment(
        database,
        tableName,
        ColumnInfo.builder()
            .name("username")
            .fieldType(DorisFieldType.Varchar)
            .precision(50)
            .comment(comment)
            .build());
  }

  @Test
  public void testModifyColumnComment() throws Exception {
    String tableName = "testModifyColumnComment";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String comment = "user name";
    TimeUnit.SECONDS.sleep(1);
    dataWarehouseDDLService.addColumnComment(
        database,
        tableName,
        ColumnInfo.builder()
            .name("username")
            .fieldType(DorisFieldType.Varchar)
            .precision(50)
            .comment(comment)
            .build());
  }

  @Test
  public void testRemoveColumnComment() {
    String tableName = "testModifyColumnComment";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    dataWarehouseDDLService.removeColumnComment(
        database,
        tableName,
        ColumnInfo.builder().name("username").fieldType(MySQLFieldType.BigInt).build());
  }

  @Test
  public void testCreateIndex() {
    String tableName = "testCreateIndex";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String indexName = "username_index";
    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());
    System.out.println();
  }

  @Test
  public void testCreateIndexIfConflict() {
    String tableName = "testCreateIndex";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String indexName = "username_index";
    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());

    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());
  }

  @Test
  public void testCreateIndexDouble() {
    String tableName = "testCreateIndex";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String indexName = "username_index";
    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName + "_" + 1)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());

    // BITMAP index for columns (username ) already exist.
    assertThrows(
        Exception.class,
        () ->
            dataWarehouseDDLService.createIndex(
                database,
                tableName,
                IndexInfo.builder()
                    .name(indexName + "_" + 2)
                    .columns(Collections.singletonList("username"))
                    .comment("user name index")
                    .build()));
  }

  @Test
  public void testDropIndex() {
    String tableName = "testDropIndex";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String indexName = "username_index";
    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName + "_" + 1)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());
    dataWarehouseDDLService.dropIndex(database, tableName, indexName);
  }

  @Test
  public void testDropIndexIfNotExists() {
    String tableName = "testDropIndex";
    dataWarehouseDDLService.createTable(tableInfoBuilder.name(tableName).build());
    String indexName = "username_index";
    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName + "_" + 1)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());
    dataWarehouseDDLService.dropIndex(database, tableName, indexName);
    dataWarehouseDDLService.dropIndex(database, tableName, indexName);
  }
}
