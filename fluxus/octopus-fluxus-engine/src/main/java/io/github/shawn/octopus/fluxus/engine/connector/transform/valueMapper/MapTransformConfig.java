package io.github.shawn.octopus.fluxus.engine.connector.transform.valueMapper;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.TransformConstants.VALUE_MAPPER_TRANSFORM;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.common.PredicateUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseTransformConfig;
import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MapTransformConfig extends BaseTransformConfig<MapTransformConfig.MapTransformOptions>
    implements TransformConfig<MapTransformConfig.MapTransformOptions> {

  private String id;
  private String name;
  @Builder.Default private String identifier = VALUE_MAPPER_TRANSFORM;
  public List<String> inputs;
  public String output;
  private MapTransformOptions options;

  @Override
  protected void checkOptions() {
    PredicateUtils.verify(
        ArrayUtils.isNotEmpty(options.getValueMappers()),
        "valueMappers in valueMapper config cannot be null");
  }

  @Override
  protected void loadTransformConfig(String json) {
    MapTransformConfig valueMapperConfig =
        JsonUtils.fromJson(json, new TypeReference<MapTransformConfig>() {})
            .orElseThrow(
                () -> new DataWorkflowException("valueMapper transform config deserialize error"));
    this.id =
        isNotBlank(valueMapperConfig.getId()) ? valueMapperConfig.getId() : IdGenerator.uuid();
    this.name = valueMapperConfig.getName();
    this.inputs = valueMapperConfig.getInputs();
    this.output = valueMapperConfig.getOutput();
    this.options = valueMapperConfig.getOptions();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MapTransformOptions implements TransformConfig.TransformOptions {

    private FieldMapper[] valueMappers;

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FieldMapper {
      private String fieldName;
      private Object[] sourceValues;
      private Object[] targetValues;
    }
  }
}
