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
import com.zaxxer.hikari.HikariDataSource;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.type.TypeAliasRegistry;
import org.jetbrains.annotations.NotNull;

public class SqlSessionProvider {

  private static final Map<String, Configuration> CONFIG_MAP = new HashMap<>(2 << 8);
  private static final TransactionFactory TX_FACTORY = new JdbcTransactionFactory();

  public static SqlSession createSqlSession(JDBCDataSourceProperties properties) {
    return createSqlSession(createConfiguration(properties));
  }

  public static SqlSession createSqlSession(Configuration configuration) {
    return new DefaultSqlSessionFactory(configuration).openSession();
  }

  public static Configuration createConfiguration(JDBCDataSourceProperties properties) {
    return createConfiguration(properties, false);
  }

  public static Configuration createConfiguration(
      JDBCDataSourceProperties properties, boolean override) {
    Configuration configuration;
    String configName = getConfigName(properties.getName());
    if ((configuration = CONFIG_MAP.get(configName)) != null) {
      // 如果不需要覆盖
      if (!override) {
        return configuration;
      }
    }
    DataSource dataSource =
        createJDBCDataSource(
            properties.getUrl(),
            properties.getUsername(),
            properties.getPassword(),
            properties.getDriverClassName());
    Environment environment =
        new Environment.Builder(getEnvId(properties.getName()))
            .dataSource(dataSource)
            .transactionFactory(TX_FACTORY)
            .build();

    configuration = new Configuration(environment);
    configuration.setMapUnderscoreToCamelCase(true);
    TypeAliasRegistry typeAliasRegistry = configuration.getTypeAliasRegistry();
    typeAliasRegistry.registerAlias("table", Table.class);
    typeAliasRegistry.registerAlias("column", Column.class);
    typeAliasRegistry.registerAlias("index", Index.class);
    typeAliasRegistry.registerAlias("databaseMeta", DatabaseMeta.class);
    typeAliasRegistry.registerAlias("tableMeta", TableMeta.class);
    typeAliasRegistry.registerAlias("columnMeta", ColumnMeta.class);
    typeAliasRegistry.registerAlias("user", User.class);
    typeAliasRegistry.registerAlias("privilege", Privilege.class);
    PageAutoDialect.registerDialectAlias("doris", MySqlDialect.class);
    configuration.addInterceptor(new PageInterceptor());
    configuration.addMapper(DorisDDLDao.class);
    configuration.addMapper(MySQLDDLDao.class);
    configuration.addMapper(DorisMetaDataDao.class);
    configuration.addMapper(MySQLMetaDataDao.class);
    configuration.addMapper(DorisDCLDao.class);
    configuration.addMapper(MySQLDCLDao.class);
    configuration.addMapper(DorisDQLDao.class);
    configuration.addMapper(MySQLDQLDao.class);
    CONFIG_MAP.put(configName, configuration);
    return configuration;
  }

  public static DataSource createJDBCDataSource(
      @NotNull String url, String username, String password, @NotNull String driverClassName) {
    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(url);
    ds.setDriverClassName(driverClassName);
    if (StringUtils.isNotBlank(username)) {
      ds.setUsername(username);
    }
    if (StringUtils.isNotBlank(password)) {
      ds.setPassword(password);
    }
    return ds;
  }

  public static String getConfigName(String name) {
    return String.format("sql_session_config_%s", name);
  }

  public static String getEnvId(String name) {
    return String.format("sql_session_env_%s", name);
  }
}
