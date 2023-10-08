package com.octopus.actus.connector.jdbc.service.mysql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.octopus.actus.connector.jdbc.model.ColumnInfo;
import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.IndexInfo;
import com.octopus.actus.connector.jdbc.model.PrimaryKeyInfo;
import com.octopus.actus.connector.jdbc.model.TableInfo;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLFieldType;
import com.octopus.actus.connector.jdbc.service.DDLTestCommon;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDDLService;
import com.octopus.actus.connector.jdbc.service.impl.AbstractSqlExecutor;
import com.octopus.actus.connector.jdbc.service.impl.DataWarehouseDDLServiceImpl;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class MySQLDDLTests extends MySQLTestsCommon {

  private DataWarehouseDDLService dataWarehouseDDLService;
  private DDLTestCommon testCommon;

  @BeforeEach
  public void init() {
    dataWarehouseDDLService = new DataWarehouseDDLServiceImpl(properties);
    testCommon = new DDLTestCommon(url, username, password, driverClass);
    DatabaseInfo databaseInfo =
        DatabaseInfo.builder()
            .name(database)
            .charsetName("utf8mb4")
            .collationName("utf8mb4_general_ci")
            .build();
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
    DatabaseInfo databaseInfo =
        DatabaseInfo.builder()
            .name(database)
            .charsetName("utf8mb4")
            .collationName("utf8mb4_general_ci")
            .build();
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
  public void testCreateMySQLTableWithNoIndex() {
    String tableName = "create_table_no_index_no_pk";
    createTable(tableName, false, false, false);

    assertTrue(testCommon.hasTable(database, tableName));

    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    assertTrue(testCommon.hasColumn(database, tableName, "username"));
    assertTrue(testCommon.hasColumn(database, tableName, "password"));
    assertTrue(testCommon.hasColumn(database, tableName, "create_time"));
    assertTrue(testCommon.hasColumn(database, tableName, "deleted"));

    assertTrue(testCommon.matchColumnType(database, tableName, "id", "BIGINT"));
    assertTrue(testCommon.matchColumnType(database, tableName, "create_time", "DATETIME"));
    assertTrue(testCommon.matchColumnType(database, tableName, "username", "VARCHAR(100)"));
    assertTrue(testCommon.matchColumnType(database, tableName, "password", "VARCHAR(100)"));
    assertTrue(testCommon.matchColumnType(database, tableName, "deleted", "TINYINT(1)"));

    assertFalse(testCommon.columnIsPk(database, tableName, "id"));
    assertFalse(testCommon.columnIsPk(database, tableName, "create_time"));
    assertFalse(testCommon.columnIsNullable(database, tableName, "username"));
    assertTrue(testCommon.columnHasDefaultValue(database, tableName, "deleted"));
  }

  @Test
  public void testCreateMySQLTableWithPrimaryKey() {
    String tableName = "create_table_no_index";
    createTable(tableName, true, true, false);

    assertTrue(testCommon.hasTable(database, tableName));

    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    assertTrue(testCommon.hasColumn(database, tableName, "username"));
    assertTrue(testCommon.hasColumn(database, tableName, "password"));
    assertTrue(testCommon.hasColumn(database, tableName, "create_time"));
    assertTrue(testCommon.hasColumn(database, tableName, "deleted"));

    assertTrue(testCommon.matchColumnType(database, tableName, "id", "BIGINT"));
    assertTrue(testCommon.matchColumnType(database, tableName, "create_time", "DATETIME"));
    assertTrue(testCommon.matchColumnType(database, tableName, "username", "VARCHAR(100)"));
    assertTrue(testCommon.matchColumnType(database, tableName, "password", "VARCHAR(100)"));
    assertTrue(testCommon.matchColumnType(database, tableName, "deleted", "TINYINT(1)"));

    assertTrue(testCommon.columnIsPk(database, tableName, "id"));
    assertFalse(testCommon.columnIsPk(database, tableName, "create_time"));
    assertFalse(testCommon.columnIsNullable(database, tableName, "username"));
    assertTrue(testCommon.columnHasDefaultValue(database, tableName, "deleted"));
  }

  @Test
  public void testCreateMySQLTableWithIndexBTREE() {
    String tableName = "create_table_with_index_btree";
    createTable(tableName, true, true, true);
    assertTrue(testCommon.hasIndex(database, tableName, "username_index"));
    assertTrue(testCommon.matchIndexType(database, tableName, "username_index", "BTREE"));
  }

  @Test
  public void testCreateTableIfExists() {
    String tableName = "test";
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasTable(database, tableName));
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testDropTable() {
    String tableName = "test";
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasTable(database, tableName));
    dataWarehouseDDLService.dropTable(database, tableName);
    assertFalse(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testDropTableIfNotExists() {
    String tableName = "test";
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasTable(database, tableName));
    dataWarehouseDDLService.dropTable(database, tableName);
    assertFalse(testCommon.hasTable(database, tableName));
    dataWarehouseDDLService.dropTable(database, tableName);
    assertFalse(testCommon.hasTable(database, tableName));
  }

  @Test
  public void testRenameTable() {
    String oldTableName = "oldTable";
    createTable(oldTableName, true, true, false);
    assertTrue(testCommon.hasTable(database, oldTableName));
    String newTableName = "newTable";
    dataWarehouseDDLService.renameTable(database, oldTableName, newTableName);
    assertFalse(testCommon.hasTable(database, oldTableName));
    assertTrue(testCommon.hasTable(database, newTableName));
  }

  @Test
  public void testAddTableComment() {
    String tableName = "testAddTableComment";
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasTable(database, tableName));
    String comment = "添加表注释";
    dataWarehouseDDLService.addTableComment(database, tableName, comment);
    assertTrue(testCommon.matchTableComment(database, tableName, comment));
  }

  @Test
  public void testModifyTableComment() {
    String tableName = "testModifyTableComment";
    String initComment = "初始表注释";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .comment(initComment)
            .build());
    assertTrue(testCommon.matchTableComment(database, tableName, initComment));
    String newComment = "修改后的表注释";
    dataWarehouseDDLService.modifyTableComment(database, tableName, newComment);
    assertFalse(testCommon.matchTableComment(database, tableName, initComment));
    assertTrue(testCommon.matchTableComment(database, tableName, newComment));
  }

  @Test
  public void testRemoveTableComment() {
    String tableName = "testRemoveTableComment";
    String initComment = "初始表注释";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .comment(initComment)
            .build());
    assertTrue(testCommon.matchTableComment(database, tableName, initComment));
    dataWarehouseDDLService.removeTableComment(database, tableName);
    assertFalse(testCommon.matchTableComment(database, tableName, initComment));
    assertTrue(testCommon.matchTableComment(database, tableName, ""));
  }

  @Test
  public void testAddColumn() {
    String tableName = "testAddColumn";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .build());
    String newColumn = "username";
    assertFalse(testCommon.hasColumn(database, tableName, newColumn));
    dataWarehouseDDLService.addColumn(
        database,
        tableName,
        ColumnInfo.builder()
            .name(newColumn)
            .fieldType(MySQLFieldType.Varchar)
            .precision(100)
            .nullable(false)
            .build());
    assertTrue(testCommon.hasColumn(database, tableName, newColumn));
    assertTrue(testCommon.matchColumnType(database, tableName, newColumn, "Varchar(100)"));
  }

  @Test
  public void testAddColumnWithDefaultValue() {
    String tableName = "testAddColumnWithDefaultValue";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .build());
    String newColumn = "password";
    assertFalse(testCommon.hasColumn(database, tableName, newColumn));
    dataWarehouseDDLService.addColumn(
        database,
        tableName,
        ColumnInfo.builder()
            .name(newColumn)
            .fieldType(MySQLFieldType.Varchar)
            .precision(100)
            .nullable(false)
            .defaultValue("123456;a")
            .build());
    assertTrue(testCommon.hasColumn(database, tableName, newColumn));
    assertTrue(testCommon.matchColumnType(database, tableName, newColumn, "Varchar(100)"));
    assertTrue(testCommon.columnHasDefaultValue(database, tableName, newColumn));
  }

  @Test
  public void testRenameColumn() {
    String tableName = "testRenameColumn";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .build());
    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    dataWarehouseDDLService.renameColumn(database, tableName, "id", "pk");
    assertFalse(testCommon.hasColumn(database, tableName, "id"));
    assertTrue(testCommon.hasColumn(database, tableName, "pk"));
  }

  @Test
  public void testModifyColumn() {
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
    dataWarehouseDDLService.renameColumn(database, tableName, "id", "pk");
    assertTrue(testCommon.hasColumn(database, tableName, "pk"));
    assertTrue(testCommon.matchColumnType(database, tableName, "pk", "Varchar(100)"));
    assertTrue(testCommon.columnHasDefaultValue(database, tableName, "pk"));
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
  public void testDropColumn() {
    String tableName = "testDropColumn";
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasColumn(database, tableName, "username"));
    dataWarehouseDDLService.dropColumn(database, tableName, "username");
    assertFalse(testCommon.hasColumn(database, tableName, "username"));
  }

  @Test
  public void testDropColumnIfNotExists() {
    String tableName = "testDropColumn";
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasColumn(database, tableName, "username"));
    Assertions.assertThrows(
        Exception.class, () -> dataWarehouseDDLService.dropColumn(database, tableName, "user"));
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
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .build());
    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    Assertions.assertThrows(
        Exception.class, () -> dataWarehouseDDLService.dropColumn(database, tableName, "id"));
  }

  @Test
  public void testDropPKColumn() {
    String tableName = "testDropPKColumn";
    createTable(tableName, true, true, false);
    assertTrue(testCommon.hasColumn(database, tableName, "id"));
    dataWarehouseDDLService.dropColumn(database, tableName, "id");
    assertFalse(testCommon.hasColumn(database, tableName, "id"));
  }

  @Test
  public void testAddColumnComment() {
    String tableName = "testAddColumnComment";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).build()))
            .build());
    assertTrue(
        testCommon.matchColumnComment(
            database, tableName, "id", AbstractSqlExecutor.BLANK_COMMENT));
    String comment = "主键";
    dataWarehouseDDLService.addColumnComment(
        database,
        tableName,
        ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).comment(comment).build());
    assertTrue(testCommon.matchColumnComment(database, tableName, "id", comment));
  }

  @Test
  public void testAddColumnCommentIfExists() {
    String tableName = "testAddColumnComment";
    String comment = "主键";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder()
                        .name("id")
                        .fieldType(MySQLFieldType.BigInt)
                        .comment(comment)
                        .build()))
            .build());
    assertTrue(testCommon.matchColumnComment(database, tableName, "id", comment));
    dataWarehouseDDLService.addColumnComment(
        database,
        tableName,
        ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).comment(comment).build());
    assertTrue(testCommon.matchColumnComment(database, tableName, "id", comment));
  }

  @Test
  public void testModifyColumnComment() {
    String tableName = "testModifyColumnComment";
    String comment = "主键";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder()
                        .name("id")
                        .fieldType(MySQLFieldType.BigInt)
                        .comment(comment)
                        .build()))
            .build());
    assertTrue(testCommon.matchColumnComment(database, tableName, "id", comment));
    dataWarehouseDDLService.modifyColumnComment(
        database,
        tableName,
        ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).comment("pk").build());
    assertTrue(testCommon.matchColumnComment(database, tableName, "id", "pk"));
  }

  @Test
  public void testRemoveColumnComment() {
    String tableName = "testModifyColumnComment";
    String comment = "主键";
    dataWarehouseDDLService.createTable(
        TableInfo.builder()
            .databaseInfo(DatabaseInfo.builder().name(database).build())
            .name(tableName)
            .columns(
                Collections.singletonList(
                    ColumnInfo.builder()
                        .name("id")
                        .fieldType(MySQLFieldType.BigInt)
                        .comment(comment)
                        .build()))
            .build());
    assertTrue(testCommon.matchColumnComment(database, tableName, "id", comment));
    dataWarehouseDDLService.removeColumnComment(
        database,
        tableName,
        ColumnInfo.builder().name("id").fieldType(MySQLFieldType.BigInt).comment("pk").build());
    assertTrue(
        testCommon.matchColumnComment(
            database, tableName, "id", AbstractSqlExecutor.BLANK_COMMENT));
  }

  @Test
  public void testCreateIndex() {
    String tableName = "testCreateIndex";
    createTable(tableName, true, true, false);
    String indexName = "username_index";
    assertFalse(testCommon.hasIndex(database, tableName, indexName));
    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());
    assertTrue(testCommon.hasIndex(database, tableName, indexName));
  }

  @Test
  public void testCreateIndexIfConflict() {
    String tableName = "testCreateIndex";
    createTable(tableName, true, true, true);
    String indexName = "username_index";
    assertTrue(testCommon.hasIndex(database, tableName, indexName));
    assertThrows(
        Exception.class,
        () ->
            dataWarehouseDDLService.createIndex(
                database,
                tableName,
                IndexInfo.builder()
                    .name(indexName)
                    .columns(Collections.singletonList("username"))
                    .comment("user name index")
                    .build()));
  }

  @Test
  public void testCreateIndexDouble() {
    String tableName = "testCreateIndex";
    createTable(tableName, true, true, true);
    String indexName = "username_index2";
    assertTrue(testCommon.hasIndex(database, tableName, "username_index"));
    dataWarehouseDDLService.createIndex(
        database,
        tableName,
        IndexInfo.builder()
            .name(indexName)
            .columns(Collections.singletonList("username"))
            .comment("user name index")
            .build());
    assertTrue(testCommon.hasIndex(database, tableName, indexName));
  }

  @Test
  public void testDropIndex() {
    String tableName = "testDropIndex";
    createTable(tableName, true, true, true);
    assertTrue(testCommon.hasIndex(database, tableName, "username_index"));
    dataWarehouseDDLService.dropIndex(database, tableName, "username_index");
    assertFalse(testCommon.hasIndex(database, tableName, "username_index"));
  }

  @Test
  public void testDropIndexIfNotExists() {
    String tableName = "testDropIndex";
    createTable(tableName, true, true, true);
    assertTrue(testCommon.hasIndex(database, tableName, "username_index"));
    dataWarehouseDDLService.dropIndex(database, tableName, "username_index");
    assertFalse(testCommon.hasIndex(database, tableName, "username_index"));

    assertThrows(
        Exception.class,
        () -> dataWarehouseDDLService.dropIndex(database, tableName, "username_index"));
  }

  private void createTable(
      String tableName, boolean autoIncrement, boolean hasPk, boolean hasIndex) {
    final TableInfo.TableInfoBuilder builder =
        TableInfo.builder()
            .databaseInfo(
                DatabaseInfo.builder()
                    .name(database)
                    .charsetName("utf8mb4")
                    .collationName("utf8mb4_general_ci")
                    .build())
            .name(tableName)
            .columns(
                Arrays.asList(
                    ColumnInfo.builder()
                        .name("id")
                        .fieldType(MySQLFieldType.BigInt)
                        .nullable(false)
                        .autoIncrement(autoIncrement)
                        .comment("主键")
                        .build(),
                    ColumnInfo.builder()
                        .name("username")
                        .fieldType(MySQLFieldType.Varchar)
                        .nullable(false)
                        .autoIncrement(false)
                        .precision(100)
                        .comment("用户名")
                        .build(),
                    ColumnInfo.builder()
                        .name("password")
                        .fieldType(MySQLFieldType.Varchar)
                        .precision(100)
                        .nullable(false)
                        .autoIncrement(false)
                        .comment("密码")
                        .defaultValue("123456;a")
                        .build(),
                    ColumnInfo.builder()
                        .name("create_time")
                        .fieldType(MySQLFieldType.DateTime)
                        .nullable(false)
                        .autoIncrement(false)
                        .defaultValue(LocalDateTime.now())
                        .comment("创建时间")
                        .build(),
                    ColumnInfo.builder()
                        .name("deleted")
                        .fieldType(MySQLFieldType.TinyInt)
                        .precision(1)
                        .nullable(false)
                        .autoIncrement(false)
                        .defaultValue(0)
                        .comment("逻辑删除标记")
                        .build()));
    if (hasPk) {
      builder.primaryKeyInfo(
          PrimaryKeyInfo.builder().name("id_pk").columns(Collections.singletonList("id")).build());
    }
    if (hasIndex) {
      builder.indexes(
          Collections.singletonList(
              IndexInfo.builder()
                  .name("username_index")
                  .columns(Collections.singletonList("username"))
                  .comment("username index")
                  .build()));
    }
    TableInfo tableInfo = builder.build();
    dataWarehouseDDLService.createTable(tableInfo);
  }
}
