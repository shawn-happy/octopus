package com.octopus.operators.kettlex.core.row.column;

import com.octopus.operators.kettlex.core.exception.KettleXConvertException;
import java.util.Date;
import org.apache.commons.lang3.time.DateFormatUtils;

public class DateColumn extends Column {

  private DateType subType = DateType.DATETIME;

  public static enum DateType {
    DATE,
    TIMESTEMP,
    DATETIME
  }

  /** 构建值为null的DateColumn，使用Date子类型为DATETIME */
  public DateColumn() {
    this((Long) null);
  }

  /** 构建值为stamp(Unix时间戳)的DateColumn，使用Date子类型为DATETIME 实际存储有date改为long的ms，节省存储 */
  public DateColumn(final Long stamp) {
    super(stamp, FieldType.Date);
  }

  /** 构建值为date(java.util.Date)的DateColumn，使用Date子类型为DATETIME */
  public DateColumn(final Date date) {
    this(date == null ? null : date.getTime());
  }

  /** 构建值为date(java.sql.Date)的DateColumn，使用Date子类型为DATE，只有日期，没有时间 */
  public DateColumn(final java.sql.Date date) {
    this(date == null ? null : date.getTime());
    this.setSubType(DateType.DATE);
  }

  /** 构建值为time(java.sql.Time)的DateColumn，使用Date子类型为TIME，只有时间，没有日期 */
  public DateColumn(final java.sql.Time time) {
    this(time == null ? null : time.getTime());
    this.setSubType(DateType.TIMESTEMP);
  }

  /** 构建值为ts(java.sql.Timestamp)的DateColumn，使用Date子类型为DATETIME */
  public DateColumn(final java.sql.Timestamp ts) {
    this(ts == null ? null : ts.getTime());
    this.setSubType(DateType.DATETIME);
  }

  @Override
  public Long asLong() {
    return (Long) this.getRawData();
  }

  @Override
  public String asString() {
    try {
      return DateFormatUtils.format(asDate(), "yyyy-MM-dd");
    } catch (Exception e) {
      throw new KettleXConvertException(String.format("Date[%s]类型不能转为String .", this.toString()));
    }
  }

  @Override
  public Date asDate() {
    if (null == this.getRawData()) {
      return null;
    }

    return new Date((Long) this.getRawData());
  }

  @Override
  public Date asDate(String dateFormat) {
    return asDate();
  }

  public DateType getSubType() {
    return subType;
  }

  public void setSubType(DateType subType) {
    this.subType = subType;
  }
}
