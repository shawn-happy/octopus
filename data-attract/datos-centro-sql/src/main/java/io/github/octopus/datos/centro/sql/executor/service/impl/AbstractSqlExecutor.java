package io.github.octopus.datos.centro.sql.executor.service.impl;

import com.alibaba.druid.DbType;
import io.github.octopus.datos.centro.sql.exception.SqlExecutorException;
import io.github.octopus.datos.centro.sql.executor.JDBCDataSourceProperties;
import io.github.octopus.datos.centro.sql.executor.SqlSessionProvider;
import io.github.octopus.datos.centro.sql.executor.dao.DataWarehouseDCLDao;
import io.github.octopus.datos.centro.sql.executor.dao.DataWarehouseDDLDao;
import io.github.octopus.datos.centro.sql.executor.dao.DataWarehouseDQLDao;
import io.github.octopus.datos.centro.sql.executor.dao.DataWarehouseMetaDataDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisDCLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisDDLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisDQLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.doris.DorisMetaDataDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLDCLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLDDLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLDQLDao;
import io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql.MySQLMetaDataDao;
import java.util.concurrent.Callable;
import org.apache.ibatis.session.SqlSession;

public abstract class AbstractSqlExecutor {

  public static final String BLANK_COMMENT = "";

  private final JDBCDataSourceProperties properties;

  protected DataWarehouseDDLDao dataWarehouseDDLDao;
  protected DataWarehouseDCLDao dataWarehouseDCLDao;
  protected DataWarehouseDQLDao dataWarehouseDQLDao;
  protected DataWarehouseMetaDataDao dataWarehouseMetaDataDao;

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
        dataWarehouseDDLDao = sqlSession.getMapper(MySQLDDLDao.class);
        dataWarehouseDCLDao = sqlSession.getMapper(MySQLDCLDao.class);
        dataWarehouseMetaDataDao = sqlSession.getMapper(MySQLMetaDataDao.class);
        dataWarehouseDQLDao = sqlSession.getMapper(MySQLDQLDao.class);
        break;
      case starrocks:
        dataWarehouseDDLDao = sqlSession.getMapper(DorisDDLDao.class);
        dataWarehouseDCLDao = sqlSession.getMapper(DorisDCLDao.class);
        dataWarehouseMetaDataDao = sqlSession.getMapper(DorisMetaDataDao.class);
        dataWarehouseDQLDao = sqlSession.getMapper(DorisDQLDao.class);
        break;
      default:
        throw new SqlExecutorException(
            String.format("the dbtype [%s] do not supported sql execute", properties.getDbType()));
    }
  }
}
