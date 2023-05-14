package com.octopus.kettlex.core.trans;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.plugin.PluginInfo;
import com.octopus.kettlex.core.plugin.PluginRegistry;
import com.octopus.kettlex.core.plugin.PluginType;
import com.octopus.kettlex.core.steps.StepMeta;
import com.octopus.kettlex.core.utils.JsonUtil;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class TransStepMeta {

  private static final String ID_TAG = "id";
  private static final String NAME_TAG = "name";
  private static final String COMMENT_TAG = "comment";
  private static final String STEP_CONFIG_TAG = "params";

  private String id;
  private String name;
  private String comment;
  @Default private final PluginType type = PluginType.STEP;
  private StepMeta stepMeta;

  public TransStepMeta(JsonNode stepNode) {
    TransStepMeta transStepMeta =
        JsonUtil.fromJson(stepNode.asText(), new TypeReference<TransStepMeta>() {})
            .orElseThrow(() -> new KettleXJSONException("error parase step json."));
    this.id = transStepMeta.id;
    this.name = transStepMeta.name;
    this.comment = transStepMeta.comment;

    PluginRegistry registry = PluginRegistry.getRegistry();
    PluginInfo plugin = registry.getPlugin(type, id);
    this.stepMeta = registry.loadClass(plugin);

    if (stepMeta != null) {
      stepMeta.loadJson(stepNode.findValue(STEP_CONFIG_TAG));
    }
  }

  private void loadFromJSON(JsonNode stepNode) {
    this.id = stepNode.findValue(ID_TAG).asText();
    this.name = stepNode.findValue(NAME_TAG).asText();
    this.comment = stepNode.findValue(COMMENT_TAG).asText();

    PluginRegistry registry = PluginRegistry.getRegistry();
    PluginInfo plugin = registry.getPlugin(type, id);
    this.stepMeta = registry.loadClass(plugin);

    if (stepMeta != null) {
      stepMeta.loadJson(stepNode.findValue(STEP_CONFIG_TAG));
    }
  }
}
