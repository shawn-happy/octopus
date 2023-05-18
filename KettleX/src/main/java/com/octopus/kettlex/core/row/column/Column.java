package com.octopus.kettlex.core.row.column;

import com.octopus.kettlex.core.row.FieldType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public abstract class Column {

  private FieldType type;
  private String name;
  private Object value;

  public abstract Boolean asBool();

  public abstract Integer asInt();

  public abstract Long asLong();

  public abstract Float asFloat();

  public abstract Double asDouble();

  public abstract String asString();

  public abstract BigInteger asBigInteger();

  public abstract BigDecimal asBigDecimal();

  public abstract Date asDate();

  public abstract Date asDate(String format);

  public abstract Instant asInstant();

  public abstract LocalDateTime asLocalDateTime();
}
