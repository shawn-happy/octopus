package com.octopus.actus.connector.jdbc.service.impl;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.JdbcProperties;
import com.octopus.actus.connector.jdbc.SqlSessionProvider;
import com.octopus.actus.connector.jdbc.dao.DCLDao;
import com.octopus.actus.connector.jdbc.dao.DDLDao;
import com.octopus.actus.connector.jdbc.dao.DQLDao;
import com.octopus.actus.connector.jdbc.dao.MetaDataDao;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisDCLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisDDLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisDQLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.doris.DorisMetaDataDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLDCLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLDDLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLDQLDao;
import com.octopus.actus.connector.jdbc.dao.dialect.mysql.MySQLMetaDataDao;
import com.octopus.actus.connector.jdbc.exception.SqlExecutorException;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionManager;

public abstract class AbstractSqlExecutor {

  public static final String BLANK_COMMENT = "";

  private final SqlSessionManager sqlSessionManager;
  private final DbType dbType;
  protected DDLDao ddlDao;
  protected DCLDao dclDao;
  protected DQLDao dqlDao;
  protected MetaDataDao metaDataDao;

  protected AbstractSqlExecutor(String name, DataSource dataSource, DbType dbType) {
    sqlSessionManager = SqlSessionProvider.createSqlSessionManager(name, dataSource);
    this.dbType = dbType;
    sqlSessionManager.startManagedSession();
    getMapper(sqlSessionManager);
  }

  protected AbstractSqlExecutor(JdbcProperties properties) {
    sqlSessionManager = SqlSessionProvider.createSqlSessionManager(properties);
    dbType = properties.getDbType();
    sqlSessionManager.startManagedSession();
    getMapper(sqlSessionManager);
  }

  protected void execute(Runnable runnable) {

    try {
      runnable.run();
    } catch (Exception e) {
      throw new SqlExecutorException("sql execute error", e);
    }
  }

  protected <T> T execute(Callable<T> callable) {
    try {
      return callable.call();
    } catch (Exception e) {
      throw new SqlExecutorException("sql execute error", e);
    }
  }

  protected DbType getDbType() {
    return dbType;
  }

  private void getMapper(SqlSession sqlSession) {
    switch (dbType) {
      case mysql:
        ddlDao = sqlSession.getMapper(MySQLDDLDao.class);
        dclDao = sqlSession.getMapper(MySQLDCLDao.class);
        metaDataDao = sqlSession.getMapper(MySQLMetaDataDao.class);
        dqlDao = sqlSession.getMapper(MySQLDQLDao.class);
        break;
      case doris:
        ddlDao = sqlSession.getMapper(DorisDDLDao.class);
        dclDao = sqlSession.getMapper(DorisDCLDao.class);
        metaDataDao = sqlSession.getMapper(DorisMetaDataDao.class);
        dqlDao = sqlSession.getMapper(DorisDQLDao.class);
        break;
      default:
        throw new SqlExecutorException(
            String.format("the dbtype [%s] do not supported sql execute", dbType));
    }
  }
}
