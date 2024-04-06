package io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField;

import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField.strategy.GeneratedFiledStrategyFactory;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;

public class GeneratedFieldTransform implements Transform<GeneratedFieldTransformConfig> {

  private final GeneratedFieldTransformConfig config;

  private final GeneratedFieldTransformConfig.GeneratedFieldTransformOptions options;

  public GeneratedFieldTransform(GeneratedFieldTransformConfig generatedFieldTransformConfig) {
    this.config = generatedFieldTransformConfig;
    this.options = generatedFieldTransformConfig.getOptions();
  }

  @Override
  public RowRecord transform(RowRecord source) throws StepExecutionException {

    List<DataWorkflowFieldType> dataWorkflowFieldTypes =
        new LinkedList<>(Arrays.asList(source.getFieldTypes()));

    List<String> fieldNames = new LinkedList<>(Arrays.asList(source.getFieldNames()));

    GeneratedFieldTransformConfig.GeneratedFieldTransformOptions.Field[] fields =
        options.getFields();

    List<Object[]> results = new LinkedList<>();

    // 遍历json里要本次要生成的类型
    // 分别在fieldsNames,fieldTypes最后一列添加新生成的值
    for (GeneratedFieldTransformConfig.GeneratedFieldTransformOptions.Field value : fields) {
      // 在filedNames最后一列添加生成值的字段名称
      String destName = value.getDestName();
      fieldNames.add(destName);

      // 在fieldsType最后一列添加生成值的字段类型（目前都是String）
      DataWorkflowFieldType basic = BasicFieldType.STRING;
      dataWorkflowFieldTypes.add(basic);
    }

    // 遍历原来的values数组 扩充每个object[]添加生成值
    while (true) {
      Object[] values = source.pollNext();
      if (ArrayUtils.isEmpty(values)) {
        break;
      }
      List<Object> objects = new LinkedList<>(Arrays.asList(values));

      for (GeneratedFieldTransformConfig.GeneratedFieldTransformOptions.Field field : fields) {
        GenerateType generateType = field.getGenerateType();
        // 根据生成类型生成值
        String genValue =
            GeneratedFiledStrategyFactory.getStrategy(generateType.name()).generated();
        objects.add(genValue);
      }
      results.add(objects.toArray(new Object[0]));
    }

    TransformRowRecord rowRecord =
        new TransformRowRecord(
            fieldNames.toArray(new String[0]),
            dataWorkflowFieldTypes.toArray(new DataWorkflowFieldType[0]));
    rowRecord.addRecords(results);
    return rowRecord;
  }

  @Override
  public GeneratedFieldTransformConfig getTransformConfig() {
    return config;
  }
}
