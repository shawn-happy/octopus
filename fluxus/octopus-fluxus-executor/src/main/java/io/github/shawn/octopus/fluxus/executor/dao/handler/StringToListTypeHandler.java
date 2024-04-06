package io.github.shawn.octopus.fluxus.executor.dao.handler;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

public class StringToListTypeHandler extends BaseTypeHandler<List<String>> {

  @Override
  public void setNonNullParameter(
      PreparedStatement ps, int i, List<String> parameter, JdbcType jdbcType) throws SQLException {
    if (CollectionUtils.isEmpty(parameter)) {
      return;
    }
    String collect = String.join(",", parameter);
    ps.setString(i, collect);
  }

  @Override
  public List<String> getNullableResult(ResultSet rs, String columnName) throws SQLException {
    String value = rs.getString(columnName);
    if (StringUtils.isBlank(value)) {
      return null;
    }
    return Arrays.asList(value.split(","));
  }

  @Override
  public List<String> getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    String value = rs.getString(columnIndex);
    if (StringUtils.isBlank(value)) {
      return null;
    }
    return Arrays.asList(value.split(","));
  }

  @Override
  public List<String> getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    String value = cs.getString(columnIndex);
    if (StringUtils.isBlank(value)) {
      return null;
    }
    return Arrays.asList(value.split(","));
  }
}
