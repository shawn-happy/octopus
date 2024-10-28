package io.github.octopus.sql.executor.core.model.curd;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class UpdateParams {
  private final Map<String, Object> updateParams = new LinkedHashMap<>();

  public UpdateParams set(Map<String, Object> updates) {
    updates.forEach(this::addUpdate);
    return this;
  }

  public UpdateParams addUpdate(String param, Object value) {
    updateParams.put(param, value);
    return this;
  }

  public Map<String, Object> getUpdateParams() {
    return updateParams;
  }

  public Set<String> getUpdateParamNames() {
    return updateParams.keySet();
  }
}
