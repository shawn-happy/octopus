package com.octopus.operators.engine.table.type;

import java.util.List;

public interface CompositeDataType extends RowDataType {

  List<RowDataType> getCompositeType();
}
