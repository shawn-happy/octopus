package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import io.github.octopus.sql.executor.plugin.api.dao.DDLDao;
import io.github.octopus.sql.executor.plugin.api.dao.MetaDataDao;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import io.github.octopus.sql.executor.plugin.api.executor.MetaDataExecutor;
import io.github.octopus.sql.executor.plugin.mysql.dao.MySQLCurdDao;
import io.github.octopus.sql.executor.plugin.mysql.dao.MySQLDDLDao;
import io.github.octopus.sql.executor.plugin.mysql.dao.MySQLMetaDataDao;
import io.github.octopus.sql.executor.plugin.mysql.dialect.MySQLJdbcType;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLMetaDataExecutor extends MetaDataExecutor {

  private final JdbcType jdbcType = MySQLJdbcType.getJdbcType();
  private final Class<? extends CurdDao> curdDaoClass = MySQLCurdDao.class;
  private final Class<? extends DDLDao> dDLDaoClass = MySQLDDLDao.class;
  private final Class<? extends MetaDataDao> metaDataDaoClass = MySQLMetaDataDao.class;

  public MySQLMetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
