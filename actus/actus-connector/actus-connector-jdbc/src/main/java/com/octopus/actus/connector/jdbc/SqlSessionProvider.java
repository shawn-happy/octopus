package com.octopus.actus.connector.jdbc;

import com.github.pagehelper.PageInterceptor;
import com.github.pagehelper.dialect.helper.MySqlDialect;
import com.github.pagehelper.page.PageAutoDialect;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisDCLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisDDLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisDQLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisMetaDataDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLDCLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLDDLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLDQLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLMetaDataDao;
import com.octopus.actus.connector.jdbc.entity.Column;
import com.octopus.actus.connector.jdbc.entity.ColumnMeta;
import com.octopus.actus.connector.jdbc.entity.DatabaseMeta;
import com.octopus.actus.connector.jdbc.entity.Index;
import com.octopus.actus.connector.jdbc.entity.Privilege;
import com.octopus.actus.connector.jdbc.entity.Table;
import com.octopus.actus.connector.jdbc.entity.TableMeta;
import com.octopus.actus.connector.jdbc.entity.User;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionManager;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.type.TypeAliasRegistry;
import org.jetbrains.annotations.NotNull;

public class SqlSessionProvider {

  private static final Map<String, SqlSessionManager> SQL_SESSION_MANAGER_MAP =
      new HashMap<>(2 << 4);
  private static final TransactionFactory TX_FACTORY = new JdbcTransactionFactory();

  public static SqlSession createSqlSession(DataSource dataSource) {
    return createSqlSession(createConfiguration(dataSource));
  }

  public static SqlSession createSqlSession(JdbcProperties properties) {
    return createSqlSession(createConfiguration(properties));
  }

  public static SqlSession createSqlSession(Configuration configuration) {
    return new DefaultSqlSessionFactory(configuration).openSession();
  }

  public static SqlSessionManager createSqlSessionManager(JdbcProperties properties) {
    SqlSessionManager sqlSessionManager = SQL_SESSION_MANAGER_MAP.get(properties.getName());
    if (sqlSessionManager != null) {
      return sqlSessionManager;
    }
    sqlSessionManager = createSqlSessionManager(createConfiguration(properties));
    SQL_SESSION_MANAGER_MAP.put(properties.getName(), sqlSessionManager);
    return sqlSessionManager;
  }

  public static SqlSessionManager createSqlSessionManager(String name, DataSource dataSource) {
    SqlSessionManager sqlSessionManager = SQL_SESSION_MANAGER_MAP.get(name);
    if (sqlSessionManager != null) {
      return sqlSessionManager;
    }
    sqlSessionManager = createSqlSessionManager(createConfiguration(dataSource));
    SQL_SESSION_MANAGER_MAP.put(name, sqlSessionManager);
    return sqlSessionManager;
  }

  public static SqlSessionManager createSqlSessionManager(Configuration configuration) {
    return SqlSessionManager.newInstance(new DefaultSqlSessionFactory(configuration));
  }

  public static Configuration createConfiguration(JdbcProperties properties) {
    return createConfiguration(
        createJDBCDataSource(
            properties.getUrl(),
            properties.getUsername(),
            properties.getPassword(),
            properties.getDriverClassName()));
  }

  public static Configuration createConfiguration(DataSource dataSource) {
    Configuration configuration;
    Environment environment =
        new Environment.Builder(UUID.randomUUID().toString())
            .dataSource(dataSource)
            .transactionFactory(TX_FACTORY)
            .build();

    configuration = new Configuration(environment);
    configuration.setMapUnderscoreToCamelCase(true);
    registerTypeAliasInternal(configuration);
    addInterceptors(configuration);
    addMappers(configuration);
    return configuration;
  }

  public static DataSource createJDBCDataSource(
      @NotNull String url, String username, String password, @NotNull String driverClassName) {
    PooledDataSource ds = new PooledDataSource();
    ds.setUrl(url);
    ds.setDriver(driverClassName);
    if (StringUtils.isNotBlank(username)) {
      ds.setUsername(username);
    }
    if (StringUtils.isNotBlank(password)) {
      ds.setPassword(password);
    }
    return ds;
  }

  private static void registerTypeAliasInternal(Configuration configuration) {
    TypeAliasRegistry typeAliasRegistry = configuration.getTypeAliasRegistry();
    typeAliasRegistry.registerAlias("table", Table.class);
    typeAliasRegistry.registerAlias("column", Column.class);
    typeAliasRegistry.registerAlias("index", Index.class);
    typeAliasRegistry.registerAlias("databaseMeta", DatabaseMeta.class);
    typeAliasRegistry.registerAlias("tableMeta", TableMeta.class);
    typeAliasRegistry.registerAlias("columnMeta", ColumnMeta.class);
    typeAliasRegistry.registerAlias("user", User.class);
    typeAliasRegistry.registerAlias("privilege", Privilege.class);
  }

  private static void addMappers(Configuration configuration) {
    configuration.addMapper(DorisDDLDao.class);
    configuration.addMapper(MySQLDDLDao.class);
    configuration.addMapper(DorisMetaDataDao.class);
    configuration.addMapper(MySQLMetaDataDao.class);
    configuration.addMapper(DorisDCLDao.class);
    configuration.addMapper(MySQLDCLDao.class);
    configuration.addMapper(DorisDQLDao.class);
    configuration.addMapper(MySQLDQLDao.class);
  }

  private static void addInterceptors(Configuration configuration) {
    PageAutoDialect.registerDialectAlias("doris", MySqlDialect.class);
    configuration.addInterceptor(new PageInterceptor());
  }
}
