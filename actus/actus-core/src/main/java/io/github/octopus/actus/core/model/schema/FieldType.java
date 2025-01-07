package io.github.octopus.actus.core.model.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

public interface FieldType {
  /** 数据类型 */
  String getDataType();

  /** 中文名称 */
  String getChName();

  /** 描述 */
  String getDescription();

  String toString();

  int getSqlType();

  /** 是否是数字类型 */
  boolean isNumeric();

  /** 是否是字符串类型 */
  boolean isString();

  /** 是否是时间日期类型 */
  boolean isDateTime();

  /**
   * 最小精度
   *
   * @return
   */
  Integer getMinPrecision();

  /**
   * 最大精度
   *
   * @return
   */
  Integer getMaxPrecision();

  Integer getMinScale();

  Integer getMaxScale();

  boolean hasPrecision();

  boolean hasScale();

  @Getter
  @Builder
  @AllArgsConstructor
  class FieldDescriptor {
    private final String chName;
    private final Integer minPrecision;
    private final Integer maxPrecision;
    private final Integer minScale;
    private final Integer maxScale;
    private final String description;

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("chName: ");
      builder.append(chName);
      builder.append(", description: ");
      builder.append(description);
      if (minPrecision != null && maxPrecision != null) {
        builder
            .append(", ")
            .append("precision range: [")
            .append(minPrecision)
            .append(", ")
            .append(maxPrecision)
            .append("]");
      }

      if (minScale != null && maxScale != null) {
        builder
            .append(", ")
            .append("scale range: [")
            .append(minScale)
            .append(", ")
            .append(maxScale)
            .append("]");
      }
      return builder.toString();
    }
  }
}
