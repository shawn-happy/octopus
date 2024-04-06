package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model;

import java.util.Objects;

public class DorisField {
  private String name;
  private String type;
  private String comment;
  private int precision;
  private int scale;
  private String aggregationType;

  public DorisField() {}

  public DorisField(
      String name, String type, String comment, int precision, int scale, String aggregationType) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.precision = precision;
    this.scale = scale;
    this.aggregationType = aggregationType;
  }

  public String getAggregationType() {
    return aggregationType;
  }

  public void setAggregationType(String aggregationType) {
    this.aggregationType = aggregationType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DorisField dorisField = (DorisField) o;
    return precision == dorisField.precision
        && scale == dorisField.scale
        && Objects.equals(name, dorisField.name)
        && Objects.equals(type, dorisField.type)
        && Objects.equals(comment, dorisField.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, comment, precision, scale);
  }

  @Override
  public String toString() {
    return "Field{"
        + "name='"
        + name
        + '\''
        + ", type='"
        + type
        + '\''
        + ", comment='"
        + comment
        + '\''
        + ", precision="
        + precision
        + ", scale="
        + scale
        + '}';
  }
}
