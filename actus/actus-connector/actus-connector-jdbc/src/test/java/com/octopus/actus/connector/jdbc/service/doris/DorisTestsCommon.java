package com.octopus.actus.connector.jdbc.service.doris;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.JDBCDataSourceProperties;
import com.octopus.actus.connector.jdbc.model.ColumnInfo;
import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.TableInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DistributionInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisDistributionArgo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisFieldType;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DorisTestsCommon {

  protected static final String url =
      "jdbc:mysql://localhost:9030/information_schema?characterEncoding=utf-8&useSSL=false";
  protected static final String username = "root";
  protected static final String password = "";
  protected static final String driverClass = "com.mysql.cj.jdbc.Driver";
  protected static final String database = "doris_test";

  protected static final DatabaseInfo databaseInfo = DatabaseInfo.builder().name(database).build();
  protected static final DistributionInfo distributionInfo =
      DistributionInfo.builder()
          .columns(Collections.singletonList("id"))
          .distributionAlgo(DorisDistributionArgo.Hash)
          .num(3)
          .build();
  protected static final List<ColumnInfo> columns =
      Arrays.asList(
          ColumnInfo.builder()
              .name("id")
              .fieldType(DorisFieldType.BigInt)
              .nullable(false)
              .autoIncrement(true)
              .comment("主键")
              .build(),
          ColumnInfo.builder()
              .name("username")
              .fieldType(DorisFieldType.Varchar)
              .nullable(false)
              .autoIncrement(false)
              .precision(100)
              .comment("用户名")
              .build(),
          ColumnInfo.builder()
              .name("password")
              .fieldType(DorisFieldType.Varchar)
              .precision(100)
              .nullable(false)
              .autoIncrement(false)
              .comment("密码")
              .defaultValue("123456;a")
              .build(),
          ColumnInfo.builder()
              .name("create_time")
              .fieldType(DorisFieldType.DateTime)
              .nullable(false)
              .autoIncrement(false)
              .defaultValue(LocalDateTime.now())
              .comment("创建时间")
              .build());

  protected static final TableInfo.TableInfoBuilder tableInfoBuilder =
      TableInfo.builder()
          .databaseInfo(databaseInfo)
          .columns(columns)
          .distributionInfo(distributionInfo);

  protected static final String table = "doris_test_table";
  protected static final TableInfo tableInfo = tableInfoBuilder.name(table).build();

  protected static final JDBCDataSourceProperties properties =
      JDBCDataSourceProperties.builder()
          .dbType(DbType.doris)
          .name("doris_test")
          .driverClassName(driverClass)
          .url(url)
          .username(username)
          .password(password)
          .build();
}
