package com.octopus.kettlex.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.steps.Verifyable;
import com.octopus.kettlex.core.utils.JsonUtil;
import java.util.Collections;
import java.util.Map;

public interface Options extends Verifyable {

  default Map<String, String> getOptions() {
    return JsonUtil.fromJson(
            JsonUtil.toJson(this)
                .orElseThrow(() -> new KettleXJSONException("parse Options error")),
            new TypeReference<Map<String, String>>() {})
        .orElse(Collections.emptyMap());
  }
}
