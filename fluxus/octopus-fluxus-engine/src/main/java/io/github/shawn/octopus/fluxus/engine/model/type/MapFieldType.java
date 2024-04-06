package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.model.type.CompositeFieldType;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MapFieldType implements CompositeFieldType {

  private final DataWorkflowFieldType keyFieldType;
  private final DataWorkflowFieldType valueFieldType;

  public MapFieldType(DataWorkflowFieldType keyFieldType, DataWorkflowFieldType valueFieldType) {
    this.keyFieldType = keyFieldType;
    this.valueFieldType = valueFieldType;
  }

  @Override
  public Class<?> getTypeClass() {
    return Map.class;
  }

  @Override
  public List<DataWorkflowFieldType> getCompositeType() {
    return Arrays.asList(keyFieldType, valueFieldType);
  }

  @Override
  public String toString() {
    return String.format("map<%s, %s>", keyFieldType, valueFieldType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MapFieldType that = (MapFieldType) o;
    return Objects.equals(keyFieldType, that.keyFieldType)
        && Objects.equals(valueFieldType, that.valueFieldType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyFieldType, valueFieldType);
  }
}
