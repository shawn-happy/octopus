package com.octopus.kettlex.core.trans;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleXTransException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;

@Getter
@Setter
@AllArgsConstructor
public class TransLinkMeta {

  private static final String LINK_FROM_TAG = "from";
  private static final String LINK_TO_TAG = "to";

  private TransStepMeta from;
  private TransStepMeta to;

  public TransLinkMeta(JsonNode jsonNode, List<TransStepMeta> stepMetas) {
    this.from = searchStep(stepMetas, jsonNode.findValue(LINK_FROM_TAG).asText());
    this.to = searchStep(stepMetas, jsonNode.findValue(LINK_TO_TAG).asText());
  }

  private TransStepMeta searchStep(List<TransStepMeta> stepMetas, String name) {
    if (CollectionUtils.isEmpty(stepMetas)) {
      return null;
    }
    List<TransStepMeta> matchStepMetas =
        stepMetas.stream()
            .filter(stepMeta -> stepMeta.getName().equalsIgnoreCase(name))
            .collect(Collectors.toUnmodifiableList());
    if (CollectionUtils.isEmpty(matchStepMetas)) {
      return null;
    }
    if (matchStepMetas.size() > 1) {
      throw new KettleXTransException(
          String.format("more than one step were found by name: [%s]", name));
    }
    return matchStepMetas.iterator().next();
  }
}
