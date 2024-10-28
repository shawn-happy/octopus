package io.github.octopus.sql.executor.core;

import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import io.github.octopus.sql.executor.core.entity.Column;
import io.github.octopus.sql.executor.core.entity.ColumnMeta;
import io.github.octopus.sql.executor.core.entity.ConstraintKeyMeta;
import io.github.octopus.sql.executor.core.entity.DatabaseMeta;
import io.github.octopus.sql.executor.core.entity.Delete;
import io.github.octopus.sql.executor.core.entity.Index;
import io.github.octopus.sql.executor.core.entity.Insert;
import io.github.octopus.sql.executor.core.entity.SchemaMeta;
import io.github.octopus.sql.executor.core.entity.Select;
import io.github.octopus.sql.executor.core.entity.Table;
import io.github.octopus.sql.executor.core.entity.TableMeta;
import io.github.octopus.sql.executor.core.entity.Update;
import io.github.octopus.sql.executor.core.entity.Upsert;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionManager;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.type.TypeAliasRegistry;

@Slf4j
public class SqlSessionProvider {
  private static Map<Object, DataSource> targetDataSources = new ConcurrentHashMap<>(16);
  private static Map<String, SqlSessionManager> sqlSessionMap = new ConcurrentHashMap<>(16);
  private static final TransactionFactory TX_FACTORY = new JdbcTransactionFactory();

  private SqlSessionProvider() {}

  public static synchronized SqlSessionManager createSqlSession(
      String name, DataSource dataSource) {
    Map<Object, DataSource> targetMap = SqlSessionProvider.targetDataSources;
    if (!targetMap.containsKey(name)) {
      targetMap.put(name, dataSource);
    }
    targetDataSources = targetMap;
    Map<String, SqlSessionManager> sqlSessions = SqlSessionProvider.sqlSessionMap;
    if (sqlSessions.containsKey(name)) {
      SqlSessionManager sqlSessionManager = sqlSessionMap.get(name);
      if (!sqlSessionManager.isManagedSessionStarted()) {
        sqlSessionManager.startManagedSession();
      }
      return sqlSessions.get(name);
    }
    Configuration configuration = createConfiguration(name, targetMap.get(name));
    SqlSessionManager sqlSessionManager =
        SqlSessionManager.newInstance(new DefaultSqlSessionFactory(configuration));
    sqlSessionManager.startManagedSession();
    sqlSessions.put(name, sqlSessionManager);
    sqlSessionMap = sqlSessions;
    return sqlSessionManager;
  }

  public static boolean isExistSqlSession(String key) {
    return sqlSessionMap.containsKey(key);
  }

  public static synchronized void releaseSqlSessionManager(String name) {
    SqlSessionManager sqlSessionManager = null;
    try {
      if (!isExistSqlSession(name)) {
        return;
      }
      Map<String, SqlSessionManager> sessionManagerMap = SqlSessionProvider.sqlSessionMap;
      sqlSessionManager = sessionManagerMap.get(name);
      sessionManagerMap.remove(name);
    } catch (Exception e) {
      if (sqlSessionManager != null) {
        sqlSessionManager.rollback();
      }
      log.error(e.getMessage());
      throw new RuntimeException(e);
    } finally {
      if (sqlSessionManager != null) {
        sqlSessionManager.commit();
        sqlSessionManager.close();
      }
    }
  }

  private static Configuration createConfiguration(String code, DataSource dataSource) {
    Configuration configuration;
    Environment environment =
        new Environment.Builder(getEnvId(code))
            .dataSource(dataSource)
            .transactionFactory(TX_FACTORY)
            .build();

    configuration = new Configuration(environment);
    configuration.setMapUnderscoreToCamelCase(true);
    addInternalTypeAlias(configuration);
    addInternalInterceptors(configuration);
    return configuration;
  }

  private static String getEnvId(String name) {
    return String.format("sql_session_env_%s", name);
  }

  private static void addInternalTypeAlias(Configuration configuration) {
    TypeAliasRegistry typeAliasRegistry = configuration.getTypeAliasRegistry();
    typeAliasRegistry.registerAlias("table", Table.class);
    typeAliasRegistry.registerAlias("column", Column.class);
    typeAliasRegistry.registerAlias("index", Index.class);
    typeAliasRegistry.registerAlias("databaseMeta", DatabaseMeta.class);
    typeAliasRegistry.registerAlias("tableMeta", TableMeta.class);
    typeAliasRegistry.registerAlias("columnMeta", ColumnMeta.class);
    typeAliasRegistry.registerAlias("select", Select.class);
    typeAliasRegistry.registerAlias("insert", Insert.class);
    typeAliasRegistry.registerAlias("upsert", Upsert.class);
    typeAliasRegistry.registerAlias("update", Update.class);
    typeAliasRegistry.registerAlias("delete", Delete.class);
    typeAliasRegistry.registerAlias("constraintKeyMeta", ConstraintKeyMeta.class);
    typeAliasRegistry.registerAlias("schemaMeta", SchemaMeta.class);
  }

  private static void addInternalInterceptors(Configuration configuration) {
    MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
    PaginationInnerInterceptor paginationInnerInterceptor = new PaginationInnerInterceptor();
    interceptor.addInnerInterceptor(paginationInnerInterceptor);
    configuration.addInterceptor(interceptor);
  }
}
