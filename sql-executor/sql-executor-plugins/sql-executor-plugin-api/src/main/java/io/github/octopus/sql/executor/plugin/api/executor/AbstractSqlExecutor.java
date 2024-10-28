package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.SqlSessionProvider;
import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import io.github.octopus.sql.executor.plugin.api.dao.DDLDao;
import io.github.octopus.sql.executor.plugin.api.dao.MetaDataDao;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import io.github.octopus.sql.executor.plugin.api.exception.SqlExecuteException;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;

@Slf4j
abstract class AbstractSqlExecutor {

  public static final String BLANK_COMMENT = "";

  private final SqlSession sqlSession;
  private final String name;
  private DDLDao ddlDao;
  private CurdDao curdDao;
  private MetaDataDao metaDataDao;

  protected AbstractSqlExecutor(String name, DataSource dataSource) {
    this.name = name;
    this.sqlSession = SqlSessionProvider.createSqlSession(name, dataSource);
    this.sqlSession.getConfiguration().addMapper(getCurdDaoClass());
    this.sqlSession.getConfiguration().addMapper(getDDLDaoClass());
    this.sqlSession.getConfiguration().addMapper(getMetaDataDaoClass());
    getMapper(sqlSession);
  }

  protected void executeDDL(Consumer<DDLDao> ddlDaoConsumer) {
    try {
      ddlDaoConsumer.accept(ddlDao);
    } catch (Exception e) {
      throw new SqlExecuteException(e);
    } finally {
      SqlSessionProvider.releaseSqlSessionManager(name);
    }
  }

  protected <R> R executeCurd(Function<CurdDao, R> function) {
    try {
      return function.apply(curdDao);
    } catch (Exception e) {
      throw new SqlExecuteException(e);
    } finally {
      SqlSessionProvider.releaseSqlSessionManager(name);
    }
  }

  protected <R> R executeMetaData(Function<MetaDataDao, R> function) {
    try {
      return function.apply(metaDataDao);
    } catch (Exception e) {
      throw new SqlExecuteException(e);
    } finally {
      SqlSessionProvider.releaseSqlSessionManager(name);
    }
  }

  protected String getSql(String statement, Object parameterObject) {
    try {
      String sql =
          sqlSession
              .getConfiguration()
              .getMappedStatement(statement)
              .getBoundSql(parameterObject)
              .getSql();
      log.info("sql: {}", sql);
      return sql;
    } catch (Exception e) {
      throw new SqlExecuteException(e);
    } finally {
      SqlSessionProvider.releaseSqlSessionManager(name);
    }
  }

  protected abstract JdbcType getJdbcType();

  protected abstract Class<? extends CurdDao> getCurdDaoClass();

  protected abstract Class<? extends DDLDao> getDDLDaoClass();

  protected abstract Class<? extends MetaDataDao> getMetaDataDaoClass();

  private void getMapper(SqlSession sqlSession) {
    curdDao = sqlSession.getMapper(getCurdDaoClass());
    ddlDao = sqlSession.getMapper(getDDLDaoClass());
    metaDataDao = sqlSession.getMapper(getMetaDataDaoClass());
  }
}
