package com.octopus.operators.engine.table.type;

import java.util.List;
import java.util.Map;

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
}
