package io.github.shawn.octopus.fluxus.executor.bo;

import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.sink.JdbcSinkStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.sink.PulsarSinkStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.source.ActiveMqSourceStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.source.PulsarSourceStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.source.RabbitMqSourceStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform.DesensitizeStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform.ExpressionStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform.GeneratedFieldStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform.JsonParseStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform.ValueMapperStepOptions;
import io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform.XmlParseStepOptions;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "fullType")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ActiveMqSourceStepOptions.class, name = "source_activemq"),
  @JsonSubTypes.Type(value = RabbitMqSourceStepOptions.class, name = "source_rabbitmq"),
  @JsonSubTypes.Type(value = PulsarSourceStepOptions.class, name = "source_pulsar"),
  @JsonSubTypes.Type(value = PulsarSinkStepOptions.class, name = "sink_pulsar"),
  @JsonSubTypes.Type(value = JdbcSinkStepOptions.class, name = "sink_jdbc"),
  @JsonSubTypes.Type(value = DesensitizeStepOptions.class, name = "transform_desensitize"),
  @JsonSubTypes.Type(value = ExpressionStepOptions.class, name = "transform_expression"),
  @JsonSubTypes.Type(value = GeneratedFieldStepOptions.class, name = "transform_generatedField"),
  @JsonSubTypes.Type(value = JsonParseStepOptions.class, name = "transform_jsonParse"),
  @JsonSubTypes.Type(value = XmlParseStepOptions.class, name = "transform_xmlParse"),
  @JsonSubTypes.Type(value = ValueMapperStepOptions.class, name = "transform_valueMapper"),
})
public interface StepOptions {

  String getIdentify();

  PluginType getPluginType();

  List<StepAttribute> getStepAttributes();

  @JsonProperty("fullType")
  default String getFullType() {
    return getPluginType().getType() + "_" + getIdentify();
  }
}
