package io.github.octopus.datos.centro.sql.executor;

import com.alibaba.druid.pool.DruidDataSource;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisDCLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisDDLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisDQLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisMetaDataDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLDCLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLDDLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLDQLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLMetaDataDao;
import io.github.octopus.datos.centro.sql.executor.entity.Column;
import io.github.octopus.datos.centro.sql.executor.entity.ColumnMeta;
import io.github.octopus.datos.centro.sql.executor.entity.DatabaseMeta;
import io.github.octopus.datos.centro.sql.executor.entity.Index;
import io.github.octopus.datos.centro.sql.executor.entity.Privilege;
import io.github.octopus.datos.centro.sql.executor.entity.Table;
import io.github.octopus.datos.centro.sql.executor.entity.TableMeta;
import io.github.octopus.datos.centro.sql.executor.entity.User;
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
    PaginationInnerInterceptor paginationInnerInterceptor = new PaginationInnerInterceptor();
    MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
    interceptor.addInnerInterceptor(paginationInnerInterceptor);
    configuration.addInterceptor(interceptor);
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
    DruidDataSource ds = new DruidDataSource();
    ds.setUrl(url);
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
