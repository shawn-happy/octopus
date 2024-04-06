package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.sink;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SinkConstants.JDBC_SINK;

import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.JdbcWriterMode;
import io.github.shawn.octopus.fluxus.executor.bo.StepAttribute;
import io.github.shawn.octopus.fluxus.executor.bo.StepOptions;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Arrays;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@JsonTypeName("sink_jdbc")
@NoArgsConstructor
@AllArgsConstructor
public class JdbcSinkStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.SINK;
  @Builder.Default private final String identify = JDBC_SINK;

  private String url;
  private String username;
  private String password;
  private String driver;
  private String database;
  private String table;
  private JdbcWriterMode mode;
  private String[] fields;
  private String[] conditionFields;
  private String[] primaryKeys;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Arrays.asList(
        StepAttribute.builder().code("username").value(getUsername()).build(),
        StepAttribute.builder().code("password").value(getPassword()).build(),
        StepAttribute.builder().code("url").value(getUrl()).build(),
        StepAttribute.builder().code("driver").value(getDriver()).build(),
        StepAttribute.builder().code("database").value(getDatabase()).build(),
        StepAttribute.builder().code("table").value(getTable()).build(),
        StepAttribute.builder().code("mode").value(getMode().name()).build(),
        StepAttribute.builder().code("fields").value(String.join(",", getFields())).build(),
        StepAttribute.builder()
            .code("conditionFields")
            .value(
                ArrayUtils.isNotEmpty(getConditionFields())
                    ? String.join(",", getConditionFields())
                    : null)
            .build(),
        StepAttribute.builder()
            .code("primaryKeys")
            .value(
                ArrayUtils.isNotEmpty(getPrimaryKeys()) ? String.join(",", getPrimaryKeys()) : null)
            .build());
  }
}
