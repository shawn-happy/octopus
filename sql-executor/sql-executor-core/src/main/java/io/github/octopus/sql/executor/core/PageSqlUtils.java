package io.github.octopus.sql.executor.core;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class PageSqlUtils {

  protected static final List<SelectItem<?>> COUNT_SELECT_ITEM =
      Collections.singletonList(
          new SelectItem<>(new Column().withColumnName("COUNT(*)")).withAlias(new Alias("total")));

  public static String autoCountSql(String sql) {
    try {
      Select select = (Select) CCJSqlParserUtil.parse(sql);
      if (select instanceof SetOperationList) {
        return lowLevelCountSql(sql);
      }

      PlainSelect plainSelect = (PlainSelect) select;
      Distinct distinct = plainSelect.getDistinct();
      GroupByElement groupBy = plainSelect.getGroupBy();
      List<OrderByElement> orderBy = plainSelect.getOrderByElements();

      if (CollectionUtils.isNotEmpty(orderBy)) {
        boolean canClean = groupBy == null;
        // 包含groupBy 不去除orderBy
        if (canClean) {
          for (OrderByElement order : orderBy) {
            // order by 里带参数,不去除order by
            Expression expression = order.getExpression();
            if (!(expression instanceof Column)
                && expression.toString().contains(StringPool.QUESTION_MARK)) {
              canClean = false;
              break;
            }
          }
        }
        if (canClean) {
          plainSelect.setOrderByElements(null);
        }
      }
      for (SelectItem<?> item : plainSelect.getSelectItems()) {
        if (item.toString().contains(StringPool.QUESTION_MARK)) {
          return lowLevelCountSql(select.toString());
        }
      }
      // 包含 distinct、groupBy不优化
      if (distinct != null || null != groupBy) {
        return lowLevelCountSql(select.toString());
      }
      // 包含 join 连表,进行判断是否移除 join 连表

      List<Join> joins = plainSelect.getJoins();
      if (CollectionUtils.isNotEmpty(joins)) {
        boolean canRemoveJoin = true;
        String whereS =
            Optional.ofNullable(plainSelect.getWhere())
                .map(Expression::toString)
                .orElse(StringPool.EMPTY);
        // 不区分大小写
        whereS = whereS.toLowerCase();
        for (Join join : joins) {
          if (!join.isLeft()) {
            canRemoveJoin = false;
            break;
          }
          FromItem rightItem = join.getRightItem();
          String str = "";
          if (rightItem instanceof Table) {
            Table table = (Table) rightItem;
            str =
                Optional.ofNullable(table.getAlias()).map(Alias::getName).orElse(table.getName())
                    + StringPool.DOT;
          } else if (rightItem instanceof ParenthesedSelect) {
            ParenthesedSelect subSelect = (ParenthesedSelect) rightItem;
            /* 如果 left join 是子查询，并且子查询里包含 ?(代表有入参) 或者 where 条件里包含使用 join 的表的字段作条件,就不移除 join */
            if (subSelect.toString().contains(StringPool.QUESTION_MARK)) {
              canRemoveJoin = false;
              break;
            }
            str = subSelect.getAlias().getName() + StringPool.DOT;
          }
          // 不区分大小写
          str = str.toLowerCase();

          if (whereS.contains(str)) {
            /* 如果 where 条件里包含使用 join 的表的字段作条件,就不移除 join */
            canRemoveJoin = false;
            break;
          }

          for (Expression expression : join.getOnExpressions()) {
            if (expression.toString().contains(StringPool.QUESTION_MARK)) {
              /* 如果 join 里包含 ?(代表有入参) 就不移除 join */
              canRemoveJoin = false;
              break;
            }
          }
        }

        if (canRemoveJoin) {
          plainSelect.setJoins(null);
        }
      }
      // 优化 SQL
      plainSelect.setSelectItems(COUNT_SELECT_ITEM);
      return select.toString();
    } catch (JSQLParserException e) {
      // 无法优化使用原 SQL
      log.warn(
          "optimize this sql to a count sql has exception, sql:\""
              + sql
              + "\", exception:\n"
              + e.getCause());
    } catch (Exception e) {
      log.warn("optimize this sql to a count sql has error, sql:\"" + sql + "\", exception:\n" + e);
    }
    return lowLevelCountSql(sql);
  }

  private static String lowLevelCountSql(String originalSql) {
    return getOriginalCountSql(originalSql);
  }

  private static String getOriginalCountSql(String originalSql) {
    return String.format("SELECT COUNT(*) FROM (%s) TOTAL", originalSql);
  }
}
