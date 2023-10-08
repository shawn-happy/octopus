package com.octopus.actus.connector.jdbc.service.mysql;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.JDBCDataSourceProperties;
import com.octopus.actus.connector.jdbc.model.ColumnInfo;
import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.PrimaryKeyInfo;
import com.octopus.actus.connector.jdbc.model.TableInfo;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLFieldType;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MySQLTestsCommon {
  protected static final String url =
      "jdbc:mysql://localhost:3306/datawarehouse?characterEncoding=utf-8&useSSL=false";
  protected static final String username = "root";
  protected static final String password = "bigdata321";
  protected static final String driverClass = "com.mysql.cj.jdbc.Driver";
  protected static final String database = "mysql_test";

  protected static final JDBCDataSourceProperties properties =
      JDBCDataSourceProperties.builder()
          .dbType(DbType.mysql)
          .name("mysql_ds")
          .driverClassName(driverClass)
          .url(url)
          .username(username)
          .password(password)
          .build();

  protected static final List<ColumnInfo> columns =
      Arrays.asList(
          ColumnInfo.builder()
              .name("id")
              .fieldType(MySQLFieldType.BigInt)
              .nullable(false)
              .autoIncrement(true)
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
              .name("update_time")
              .fieldType(MySQLFieldType.DateTime)
              .nullable(false)
              .autoIncrement(false)
              .defaultValue(LocalDateTime.now())
              .comment("更新时间")
              .build());

  protected static final TableInfo TABLE_INFO =
      TableInfo.builder()
          .databaseInfo(DatabaseInfo.builder().name(database).build())
          .name("mysql_meta_test")
          .columns(columns)
          .comment("mysql metadata test")
          .primaryKeyInfo(
              PrimaryKeyInfo.builder()
                  .columns(Collections.singletonList("id"))
                  .name("id_pk")
                  .build())
          .build();
}
