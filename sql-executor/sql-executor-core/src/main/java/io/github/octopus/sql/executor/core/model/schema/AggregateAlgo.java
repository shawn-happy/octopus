package io.github.octopus.sql.executor.core.model.schema;

import java.util.Arrays;

public enum AggregateAlgo {
  /** SUM：求和。适用数值类型。 */
  SUM("SUM"),
  /** MIN：求最小值。适合数值类型。 */
  MIN("MIN"),
  /** MAX：求最大值。适合数值类型。 */
  MAX("MAX"),
  /** REPLACE：替换。对于维度列相同的行，指标列会按照导入的先后顺序，后导入的替换先导入的。 */
  REPLACE("REPLACE"),
  /**
   * REPLACE_IF_NOT_NULL：非空值替换。和 REPLACE
   * 的区别在于对于null值，不做替换。这里要注意的是字段默认值要给NULL，而不能是空字符串，如果是空字符串，会给你替换成空字符串。
   */
  REPLACE_IF_NOT_NULL("REPLACE_IF_NOT_NULL"),
  /** HLL_UNION：HLL 类型的列的聚合方式，通过 HyperLogLog 算法聚合。 */
  HLL_UNION("HLL_UNION"),
  /** BITMAP_UNION：BIMTAP 类型的列的聚合方式，进行位图的并集聚合。 */
  BITMAP_UNION("BITMAP_UNION"),
  ;

  private final String algo;

  AggregateAlgo(String algo) {
    this.algo = algo;
  }

  public String getAlgo() {
    return algo;
  }

  public static AggregateAlgo of(String algo) {
    return Arrays.stream(values())
        .filter(aggregateAlgo -> aggregateAlgo.getAlgo().equalsIgnoreCase(algo))
        .findFirst()
        .orElseThrow(
            () -> new IllegalStateException(String.format("No Such Aggregate Type: [%s]", algo)));
  }
}
