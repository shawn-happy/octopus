package com.shawn.octopus.spark.etl.core.model;

import com.google.common.base.Objects;
import com.shawn.octopus.spark.etl.core.api.Hop;
import com.shawn.octopus.spark.etl.core.api.Operation;

public class DefaultHop implements Hop {

  private final Operation from;
  private final Operation to;

  public DefaultHop(Operation from, Operation to) {
    this.from = from;
    this.to = to;
  }

  @Override
  public Operation getFrom() {
    return from;
  }

  @Override
  public Operation getTo() {
    return to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DefaultHop defaultHop = (DefaultHop) o;
    return Objects.equal(from, defaultHop.from) && Objects.equal(to, defaultHop.to);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(from, to);
  }

  @Override
  public String toString() {
    return "Hop{" + getId() + '}';
  }
}
