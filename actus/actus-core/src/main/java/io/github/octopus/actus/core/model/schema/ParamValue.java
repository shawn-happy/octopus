package io.github.octopus.actus.core.model.schema;

import io.github.octopus.actus.core.model.op.InternalRelationalOp;
import io.github.octopus.actus.core.model.op.RelationalOp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ParamValue {
  private String param;
  private Object value;
  private FieldType fieldType;
  /** values只针对 In/Not In操作符 */
  private Object[] values;

  private List<String> paramIndexes;
  private Map<String, Object> paramIndexValue;

  public void multiParamName(RelationalOp op) {
    if (op != InternalRelationalOp.IN && op != InternalRelationalOp.NOT_IN) {
      return;
    }
    if (ArrayUtils.isNotEmpty(values)) {
      paramIndexes = new ArrayList<>();
      paramIndexValue = new HashMap<>();
      for (int i = 0; i < values.length; i++) {
        String paramIndex = paramIndexFormat(i);
        paramIndexes.add(paramIndex);
        paramIndexValue.put(paramIndex, values[i]);
      }
    }
  }

  private String paramIndexFormat(int index) {
    return String.format("%s_%d", param, index);
  }
}
