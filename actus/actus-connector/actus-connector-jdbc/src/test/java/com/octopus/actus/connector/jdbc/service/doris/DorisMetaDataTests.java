package com.octopus.actus.connector.jdbc.service.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.octopus.actus.connector.jdbc.model.ColumnKey;
import com.octopus.actus.connector.jdbc.model.ColumnMetaInfo;
import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.FieldType;
import com.octopus.actus.connector.jdbc.model.TableEngine;
import com.octopus.actus.connector.jdbc.model.TableMetaInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisColumnKey;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisFieldType;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisTableEngine;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDDLService;
import com.octopus.actus.connector.jdbc.service.DataWarehouseMetaDataService;
import com.octopus.actus.connector.jdbc.service.impl.DataWarehouseDDLServiceImpl;
import com.octopus.actus.connector.jdbc.service.impl.DataWarehouseMetaDataServiceImpl;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class DorisMetaDataTests extends DorisTestsCommon {

  private DataWarehouseDDLService dataWarehouseDDLService;
  private DataWarehouseMetaDataService dataWarehouseMetaDataService;

  private static final List<String> internalSchemas =
      ImmutableList.of("information_schema", "__internal_schema");

  @BeforeEach
  public void init() {
    dataWarehouseDDLService = new DataWarehouseDDLServiceImpl(properties);
    dataWarehouseMetaDataService = new DataWarehouseMetaDataServiceImpl(properties);
    DatabaseInfo databaseInfo = DatabaseInfo.builder().name(database).build();
    dataWarehouseDDLService.createDatabase(databaseInfo);
    dataWarehouseDDLService.createTable(tableInfo);
  }

  @AfterEach
  public void destroy() {
    dataWarehouseDDLService.dropDatabase(database);
  }

  @Test
  public void testGetDatabaseMetas() {
    final List<DatabaseInfo> databaseInfos = dataWarehouseMetaDataService.getDatabaseInfos();
    assertNotNull(databaseInfos);
    databaseInfos.forEach(
        databaseInfo -> assertFalse(internalSchemas.contains(databaseInfo.getName())));
    final List<String> databases =
        databaseInfos.stream().map(DatabaseInfo::getName).collect(Collectors.toList());
    assertTrue(databases.contains(database));
  }

  @Test
  public void testGetDatabaseMetaByDatabase() {
    final DatabaseInfo databaseInfo = dataWarehouseMetaDataService.getDatabaseInfo(database);
    assertEquals(database, databaseInfo.getName());
  }

  @Test
  public void testGetTableMetas() {
    final List<TableMetaInfo> tableInfos = dataWarehouseMetaDataService.getTableInfos();
    assertNotNull(tableInfos);
    final Set<String> databases =
        tableInfos.stream().map(TableMetaInfo::getDatabaseName).collect(Collectors.toSet());
    assertTrue(CollectionUtils.isEmpty(CollectionUtils.intersection(internalSchemas, databases)));
    final Map<String, List<TableMetaInfo>> databaseTables =
        tableInfos.stream().collect(Collectors.groupingBy(TableMetaInfo::getDatabaseName));
    assertTrue(databaseTables.containsKey(database));
    final List<TableMetaInfo> tableMetaInfos = databaseTables.get(database);
    assertNotNull(
        tableMetaInfos.stream()
            .filter(info -> info.getTableName().equals(table))
            .findFirst()
            .orElse(null));
  }

  @Test
  public void testGetTableMetasByDatabase() {
    final List<TableMetaInfo> tableInfos = dataWarehouseMetaDataService.getTableInfos(database);
    assertNotNull(tableInfos);
    assertEquals(1, tableInfos.size());
    final TableMetaInfo tableMetaInfo = tableInfos.iterator().next();
    final String tableName = tableMetaInfo.getTableName();
    assertEquals(table, tableName);
    final String comment = tableMetaInfo.getComment();
    assertEquals("test", comment);
    final TableEngine engine = tableMetaInfo.getEngine();
    assertEquals(DorisTableEngine.OLAP, engine);
  }

  @Test
  public void testGetTableMeta() {
    final TableMetaInfo tableMetaInfo = dataWarehouseMetaDataService.getTableInfo(database, table);
    final String tableName = tableMetaInfo.getTableName();
    assertEquals(table, tableName);
    final String comment = tableMetaInfo.getComment();
    assertEquals("test", comment);
    final TableEngine engine = tableMetaInfo.getEngine();
    assertEquals(DorisTableEngine.OLAP, engine);
  }

  @Test
  public void testGetColumnMetas() {
    final List<ColumnMetaInfo> columnInfos = dataWarehouseMetaDataService.getColumnInfos();
    assertNotNull(columnInfos);
    final Set<String> databases =
        columnInfos.stream().map(ColumnMetaInfo::getDatabaseName).collect(Collectors.toSet());
    assertTrue(CollectionUtils.isEmpty(CollectionUtils.intersection(internalSchemas, databases)));
    final List<ColumnMetaInfo> mysqlTestColumns =
        columnInfos.stream()
            .filter(info -> info.getDatabaseName().equalsIgnoreCase(database))
            .collect(Collectors.toList());
    for (ColumnMetaInfo mysqlTestColumn : mysqlTestColumns) {
      final String columnName = mysqlTestColumn.getColumnName();
      if ("id".equalsIgnoreCase(columnName)) {
        final ColumnKey columnKey = mysqlTestColumn.getColumnKey();
        assertEquals(DorisColumnKey.DUPLICATE_KEY, columnKey);
      }
      if ("username".equalsIgnoreCase(columnName)) {
        final FieldType fieldType = mysqlTestColumn.getFieldType();
        assertEquals(DorisFieldType.Varchar, fieldType);
        assertFalse(mysqlTestColumn.isNullable());
      }
    }
  }

  @Test
  public void testGetColumnMetasByDatabase() {
    final List<ColumnMetaInfo> columnInfos = dataWarehouseMetaDataService.getColumnInfos(database);
    assertNotNull(columnInfos);
    for (ColumnMetaInfo mysqlTestColumn : columnInfos) {
      final String columnName = mysqlTestColumn.getColumnName();
      if ("id".equalsIgnoreCase(columnName)) {
        final ColumnKey columnKey = mysqlTestColumn.getColumnKey();
        assertEquals(DorisColumnKey.DUPLICATE_KEY, columnKey);
      }
      if ("username".equalsIgnoreCase(columnName)) {
        final FieldType fieldType = mysqlTestColumn.getFieldType();
        assertEquals(DorisFieldType.Varchar, fieldType);
        assertFalse(mysqlTestColumn.isNullable());
      }
    }
  }

  @Test
  public void testGetColumnMetasByDatabaseAndTable() {
    final List<ColumnMetaInfo> columnInfos =
        dataWarehouseMetaDataService.getColumnInfos(database, table);
    assertNotNull(columnInfos);
    for (ColumnMetaInfo mysqlTestColumn : columnInfos) {
      final String columnName = mysqlTestColumn.getColumnName();
      if ("id".equalsIgnoreCase(columnName)) {
        final ColumnKey columnKey = mysqlTestColumn.getColumnKey();
        assertEquals(DorisColumnKey.DUPLICATE_KEY, columnKey);
      }
      if ("username".equalsIgnoreCase(columnName)) {
        final FieldType fieldType = mysqlTestColumn.getFieldType();
        assertEquals(DorisFieldType.Varchar, fieldType);
        assertFalse(mysqlTestColumn.isNullable());
      }
    }
  }

  @Test
  public void testGetColumnMetaByColumn() {
    final ColumnMetaInfo columnInfo =
        dataWarehouseMetaDataService.getColumnInfo(database, table, "id");
    final ColumnKey columnKey = columnInfo.getColumnKey();
    assertEquals(DorisColumnKey.DUPLICATE_KEY, columnKey);
    final FieldType fieldType = columnInfo.getFieldType();
    assertEquals(DorisFieldType.BigInt, fieldType);
  }
}
