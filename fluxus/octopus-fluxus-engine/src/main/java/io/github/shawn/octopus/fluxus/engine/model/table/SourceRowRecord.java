package io.github.shawn.octopus.fluxus.engine.model.table;

import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SourceRowRecord implements RowRecord {
  @Getter private String[] fieldNames;
  @Getter private DataWorkflowFieldType[] fieldTypes;
  private Object[] values;
  private transient List<Object[]> records;

  @Override
  public List<Object[]> getValues() {
    if (ArrayUtils.isEmpty(values)) {
      return null;
    }
    if (CollectionUtils.isEmpty(records)) {
      records = Collections.singletonList(values);
    }
    return records;
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public Object[] pollNext() {
    Object[] record = values;
    values = null;
    return record;
  }
}
