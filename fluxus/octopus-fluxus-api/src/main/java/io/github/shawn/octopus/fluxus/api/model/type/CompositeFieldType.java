package io.github.shawn.octopus.fluxus.api.model.type;

import java.util.List;

public interface CompositeFieldType extends DataWorkflowFieldType {
  List<DataWorkflowFieldType> getCompositeType();
}
