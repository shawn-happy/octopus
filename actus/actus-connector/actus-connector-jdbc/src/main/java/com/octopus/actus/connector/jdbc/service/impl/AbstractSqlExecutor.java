package com.octopus.actus.connector.jdbc.service.impl;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.JDBCDataSourceProperties;
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
import org.apache.ibatis.session.SqlSession;

public abstract class AbstractSqlExecutor {

  public static final String BLANK_COMMENT = "";

  private final JDBCDataSourceProperties properties;

  protected DDLDao ddlDao;
  protected DCLDao dclDao;
  protected DQLDao dqlDao;
  protected MetaDataDao metaDataDao;

  protected AbstractSqlExecutor(JDBCDataSourceProperties properties) {
    this.properties = properties;
  }

  protected void execute(Runnable runnable) {
    try (SqlSession sqlSession = SqlSessionProvider.createSqlSession(properties)) {
      getMapper(sqlSession);
      runnable.run();
    } catch (Exception e) {
      throw new SqlExecutorException("sql execute error", e);
    }
  }

  protected <T> T execute(Callable<T> callable) {
    try (SqlSession sqlSession = SqlSessionProvider.createSqlSession(properties)) {
      getMapper(sqlSession);
      return callable.call();
    } catch (Exception e) {
      throw new SqlExecutorException("sql execute error", e);
    }
  }

  protected DbType getDbType() {
    return properties.getDbType();
  }

  private void getMapper(SqlSession sqlSession) {
    switch (properties.getDbType()) {
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
            String.format("the dbtype [%s] do not supported sql execute", properties.getDbType()));
    }
  }
}
