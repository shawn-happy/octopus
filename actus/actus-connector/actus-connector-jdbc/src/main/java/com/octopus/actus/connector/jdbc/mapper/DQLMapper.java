package com.octopus.actus.connector.jdbc.mapper;

import com.octopus.actus.connector.jdbc.model.ParamValue;
import com.octopus.actus.connector.jdbc.model.SelectStatement;
import com.octopus.actus.connector.jdbc.model.WhereExpression;
import com.octopus.actus.connector.jdbc.model.WhereLogicalExpression;
import com.octopus.actus.connector.jdbc.model.WhereSimpleExpression;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

public class DQLMapper {

  public static Map<String, Object> toParamMap(SelectStatement statement) {
    if (statement == null) {
      return null;
    }
    WhereExpression where = statement.getWhere();
    return toParamMap(where);
  }

  public static Map<String, Object> toParamMap(WhereExpression where) {
    if (where == null) {
      return null;
    }
    Map<String, Object> params = new HashMap<>();
    if (where instanceof WhereSimpleExpression) {
      ParamValue[] pvs = ((WhereSimpleExpression) where).getParamValues();
      for (ParamValue pv : pvs) {
        String[] values = pv.getValues();
        if (ArrayUtils.isEmpty(values)) {
          continue;
        }
        if (values.length == 1) {
          params.put(pv.getParam(), values[0]);
        } else {
          params.put(pv.getParam(), values);
        }
      }
    } else if (where instanceof WhereLogicalExpression) {
      List<WhereExpression> leftExpressions = ((WhereLogicalExpression) where).getLeftExpressions();
      if (CollectionUtils.isEmpty(leftExpressions)) {
        return params;
      }
      for (WhereExpression leftExpression : leftExpressions) {
        Map<String, Object> leftParamMap = toParamMap(leftExpression);
        if (MapUtils.isNotEmpty(leftParamMap)) {
          params.putAll(leftParamMap);
        }
      }
      List<WhereExpression> rightExpressions =
          ((WhereLogicalExpression) where).getRightExpressions();
      if (CollectionUtils.isEmpty(rightExpressions)) {
        return params;
      }
      for (WhereExpression rightExpression : rightExpressions) {
        Map<String, Object> rightParamMap = toParamMap(rightExpression);
        if (MapUtils.isNotEmpty(rightParamMap)) {
          params.putAll(rightParamMap);
        }
      }
    }
    return params;
  }
}
