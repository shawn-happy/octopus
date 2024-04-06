package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DorisSchema {
  private int status = 0;
  private String keysType;
  private List<DorisField> properties;

  public DorisSchema() {
    properties = new ArrayList<>();
  }

  public DorisSchema(int fieldCount) {
    properties = new ArrayList<>(fieldCount);
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getKeysType() {
    return keysType;
  }

  public void setKeysType(String keysType) {
    this.keysType = keysType;
  }

  public List<DorisField> getProperties() {
    return properties;
  }

  public void setProperties(List<DorisField> properties) {
    this.properties = properties;
  }

  public void put(
      String name, String type, String comment, int scale, int precision, String aggregationType) {
    properties.add(new DorisField(name, type, comment, scale, precision, aggregationType));
  }

  public void put(DorisField f) {
    properties.add(f);
  }

  public DorisField get(int index) {
    if (index >= properties.size()) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Fields size:" + properties.size());
    }
    return properties.get(index);
  }

  public int size() {
    return properties.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DorisSchema dorisSchema = (DorisSchema) o;
    return status == dorisSchema.status && Objects.equals(properties, dorisSchema.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, properties);
  }

  @Override
  public String toString() {
    return "Schema{" + "status=" + status + ", properties=" + properties + '}';
  }
}
