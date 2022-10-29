package com.shawn.octopus.spark.etl.core.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.shawn.octopus.spark.etl.core.api.Hop;
import com.shawn.octopus.spark.etl.core.api.Operation;
import com.shawn.octopus.spark.etl.core.api.Transformation;
import java.util.LinkedList;
import java.util.List;

public class DefaultTransformation implements Transformation {

  private final String id;
  private final List<Operation> operations = new LinkedList<>();
  private final List<Hop> hops = new LinkedList<>();

  public DefaultTransformation(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public List<Operation> getOperations() {
    return ImmutableList.copyOf(operations);
  }

  @Override
  public List<Hop> getHops() {
    return ImmutableList.copyOf(hops);
  }

  public Operation createOperation(Operation operation) {
    operations.add(operation);
    return operation;
  }

  public Hop createHop(Operation from, Operation to) {
    Preconditions.checkArgument(operations.contains(from), "!operations.contains(from)");
    Preconditions.checkArgument(operations.contains(to), "!operations.contains(to)");
    Preconditions.checkArgument(from != to, "from == to");
    Hop hop = new DefaultHop(from, to);

    Preconditions.checkState(
        hops.stream().noneMatch(it -> it.getFrom() == from && it.getTo() == to),
        "Hop from %s to %s already exists",
        from,
        to);

    hops.add(hop);

    return hop;
  }
}
