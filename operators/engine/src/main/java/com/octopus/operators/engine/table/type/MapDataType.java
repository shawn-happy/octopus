package com.octopus.operators.engine.table.type;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MapDataType implements CompositeDataType {

  private final RowDataType keyRowDataType;
  private final RowDataType valueRowDataType;

  public MapDataType(RowDataType keyRowDataType, RowDataType valueRowDataType) {
    this.keyRowDataType = keyRowDataType;
    this.valueRowDataType = valueRowDataType;
  }

  public RowDataType getKeyRowDataType() {
    return keyRowDataType;
  }

  public RowDataType getValueRowDataType() {
    return valueRowDataType;
  }

  @Override
  public List<RowDataType> getCompositeType() {
    return List.of(keyRowDataType, valueRowDataType);
  }

  @Override
  public Class<?> getTypeClass() {
    return Map.class;
  }

  @Override
  public String toString() {
    return String.format("map<%s, %s>", keyRowDataType, valueRowDataType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MapDataType that = (MapDataType) o;
    return Objects.equals(keyRowDataType, that.keyRowDataType)
        && Objects.equals(valueRowDataType, that.valueRowDataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyRowDataType, valueRowDataType);
  }
}
