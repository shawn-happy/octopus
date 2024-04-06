package io.github.shawn.octopus.fluxus.engine.connector.transform.valueMapper;

import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

public class MapTransform implements Transform<MapTransformConfig> {

  private final MapTransformConfig config;
  private final MapTransformConfig.MapTransformOptions options;

  public MapTransform(MapTransformConfig config) {
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public RowRecord transform(RowRecord source) throws StepExecutionException {
    List<Object[]> values = source.getValues();
    if (CollectionUtils.isEmpty(values)) {
      return null;
    }
    MapTransformConfig.MapTransformOptions.FieldMapper[] valueMappers = options.getValueMappers();
    TransformRowRecord newRecord =
        new TransformRowRecord(source.getFieldNames(), source.getFieldTypes());
    for (MapTransformConfig.MapTransformOptions.FieldMapper valueMapper : valueMappers) {
      String fieldName = valueMapper.getFieldName();
      int index = source.indexOf(fieldName);
      // if exists
      if (index >= 0) {
        while (true) {
          Object[] record = source.pollNext();
          if (ArrayUtils.isEmpty(record)) {
            break;
          }
          Object value = record[index];
          Object[] sourceValues = valueMapper.getSourceValues();
          for (int i = 0; i < sourceValues.length; i++) {
            Object sourceValue = sourceValues[i];
            Object targetValue = valueMapper.getTargetValues()[i];
            if (sourceValue.equals(value)) {
              record[index] = targetValue;
              break;
            }
          }
          newRecord.addRecord(record);
        }
      }
    }

    return newRecord;
  }

  @Override
  public MapTransformConfig getTransformConfig() {
    return config;
  }
}
