package com.octopus.kettlex.core.trans;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.exception.KettleXTransException;
import com.octopus.kettlex.core.utils.JsonUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@NoArgsConstructor
public class TransMeta {

  private static final String TRANSFORMATION_TAG = "transformation";
  private static final String TRANSFORMATION_ID_TAG = "id";
  private static final String TRANSFORMATION_NAME_TAG = "name";
  private static final String TRANSFORMATION_CODE_TAG = "code";
  private static final String TRANSFORMATION_DESCRIPTION_TAG = "description";
  private static final String TRANSFORMATION_MODEL_TAG = "model";
  private static final String STEPS_TAG = "steps";
  private static final String LINKS_TAG = "links";

  public static enum TransExecutionModel {
    MULTI_THREAD,
    SINGLE_THREAD,
    YARN,
    K8S;

    public static TransExecutionModel of(String model) {
      return Arrays.stream(values())
          .filter(tm -> tm.name().equalsIgnoreCase(model))
          .findFirst()
          .orElseThrow(
              () ->
                  new KettleXTransException(
                      "unsupported transformation execution model: " + model));
    }
  }

  private String id;
  private String name;
  private String code;
  private String description;
  private TransExecutionModel model;
  private List<TransStepMeta> steps;
  private List<TransLinkMeta> links;

  public TransMeta(String json) {
    log.info("transformation json: {}", json);
    JsonNode transformationJsonNode =
        JsonUtil.toJsonNode(json)
            .orElseThrow(() -> new KettleXJSONException("error parase transformation json."));
    TransMeta transMeta =
        JsonUtil.fromJson(
            transformationJsonNode.asText(),
            new TypeReference<TransMeta>() {},
            () -> new KettleXJSONException("error parase transformation json."));
    this.id = transMeta.id;
    this.name = transMeta.name;
    this.code = transMeta.code;
    this.description = transMeta.description;
    this.model = transMeta.model;
    this.steps = transMeta.steps;
    this.links = transMeta.links;
  }

  private void loadFromJson(String json) {
    log.info("transformation json: {}", json);
    JsonNode transformationJsonNode =
        JsonUtil.toJsonNode(json)
            .orElseThrow(() -> new KettleXJSONException("error parase transformation json."));
    this.id = transformationJsonNode.findValue(TRANSFORMATION_ID_TAG).asText();
    this.name = transformationJsonNode.findValue(TRANSFORMATION_NAME_TAG).asText();
    this.code = transformationJsonNode.findValue(TRANSFORMATION_CODE_TAG).asText();
    this.description = transformationJsonNode.findValue(TRANSFORMATION_DESCRIPTION_TAG).asText();
    this.model =
        TransExecutionModel.of(
            transformationJsonNode
                .findValue(TRANSFORMATION_MODEL_TAG)
                .asText(TransExecutionModel.MULTI_THREAD.name()));
    JsonNode stepsNode = transformationJsonNode.findValue(STEPS_TAG);
    if (stepsNode.isArray()) {
      ArrayNode stepArrayNode = (ArrayNode) stepsNode;
      if (!stepArrayNode.isEmpty()) {
        this.steps = new ArrayList<>(stepArrayNode.size());
        for (int i = 0; i < stepArrayNode.size(); i++) {
          JsonNode stepNode = stepArrayNode.get(i);
          this.steps.add(
              JsonUtil.fromJson(stepNode.asText(), new TypeReference<TransStepMeta>() {})
                  .orElseThrow(() -> new KettleXJSONException("parse trans meta json error")));
        }
      }
    } else {
      throw new KettleXJSONException("steps json node is not array");
    }

    JsonNode linksNode = transformationJsonNode.findValue(LINKS_TAG);
    if (linksNode.isArray()) {
      ArrayNode linksArrayNode = (ArrayNode) linksNode;
      if (!linksArrayNode.isEmpty()) {
        this.links = new ArrayList<>(linksArrayNode.size());
        for (int i = 0; i < linksArrayNode.size(); i++) {
          JsonNode linkNode = linksArrayNode.get(i);
          this.links.add(new TransLinkMeta(linkNode, this.steps));
        }
      }
    } else {
      throw new KettleXJSONException("links json node is not array");
    }
  }
}
