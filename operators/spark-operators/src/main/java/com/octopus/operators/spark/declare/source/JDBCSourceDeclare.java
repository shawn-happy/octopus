package com.octopus.operators.spark.declare.source;

import com.octopus.operators.spark.declare.common.FieldType;
import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.source.JDBCSourceDeclare.JDBCSourceOptions;
import java.sql.Driver;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JDBCSourceDeclare implements SourceDeclare<JDBCSourceOptions> {

  @Default private final SourceType type = SourceType.jdbc;
  private JDBCSourceOptions options;
  private String name;
  private String output;
  private Integer repartition;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class JDBCSourceOptions implements SourceOptions {
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private String query;
    private List<ParamValue> params;
    private String jdbcType;
    private String partitionColumn;
    private String lowerBound;
    private String upperBound;
    private String[] partitionExpressions;
    private Integer numPartitions;
    @Default private int fetchSize = 1000;

    public String getDriverClassName() {
      if (StringUtils.isBlank(driverClassName)) {
        ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class);
        Iterator<Driver> iterator = sl.iterator();
        if (!iterator.hasNext()) {
          throw new IllegalArgumentException("driver class not found");
        }
        driverClassName = sl.iterator().next().getClass().getName();
      }
      return driverClassName;
    }

    @Override
    public Map<String, String> getOptions() {
      Map<String, String> connectProperties = new HashMap<>();
      connectProperties.put("user", username);
      connectProperties.put("password", password);
      connectProperties.put("driver", getDriverClassName());
      connectProperties.put("fetchsize", String.valueOf(fetchSize));
      if (StringUtils.isNotBlank(partitionColumn)) {
        connectProperties.put("partitionColumn", partitionColumn);
        connectProperties.put("lowerBound", lowerBound);
        connectProperties.put("upperBound", upperBound);
        connectProperties.put("numPartitions", String.valueOf(numPartitions));
      }
      return connectProperties;
    }

    @Override
    public void verify() {
      Verify.verify(StringUtils.isNotBlank(url), "url can not be empty or null in jdbc source");
      Verify.verify(
          (StringUtils.isBlank(partitionColumn)
                  && StringUtils.isBlank(lowerBound)
                  && StringUtils.isBlank(upperBound))
              || (StringUtils.isNotBlank(partitionColumn)
                  && (StringUtils.isNotBlank(lowerBound)
                      || StringUtils.isNotBlank(upperBound)
                      || (ObjectUtils.isNotEmpty(numPartitions) && numPartitions >= 1))),
          "if partitionColumn is not null, lowerBound,upperBound and numPartitions can not be null or empty in jdbc source. [lowerBound: %s, upperBound: %s, numPartitions: %d]",
          lowerBound,
          upperBound,
          numPartitions);
      Verify.verify(
          StringUtils.isNotBlank(getDriverClassName()),
          "driver class can not be empty or null in jdbc source");
    }

    @Builder
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ParamValue {
      private FieldType type;
      private Object value;
    }
  }
}
