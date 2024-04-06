package io.github.shawn.octopus.fluxus.engine.connector.transform.expression;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import io.github.shawn.octopus.data.fluxus.engine.connector.transform.expression.function.*;
import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.connector.transform.expression.function.*;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.DataWorkflowFieldTypeParse;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExpressionTransform implements Transform<ExpressionTransformConfig> {

  private final ExpressionTransformConfig config;
  private final ExpressionTransformConfig.ExpressionOptions options;
  private Map<String, Expression> compiledMap;

  public ExpressionTransform(ExpressionTransformConfig config) {
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public void init() throws StepExecutionException {
    AviatorEvaluator.addFunction(new StrConcatFunction());
    AviatorEvaluator.addFunction(new DateFormatFunction());
    AviatorEvaluator.addFunction(new SignFunction());
    AviatorEvaluator.addFunction(new ArrayFunction());
    AviatorEvaluator.addFunction(new NotInFunction());
    ExpressionTransformConfig.ExpressionField[] fields = options.getExpressionFields();
    compiledMap =
        Arrays.stream(fields)
            .map(ExpressionTransformConfig.ExpressionField::getExpression)
            .collect(Collectors.toMap(Function.identity(), AviatorEvaluator::compile));
  }

  @Override
  public RowRecord transform(RowRecord source) throws StepExecutionException {
    ExpressionTransformConfig.ExpressionField[] fields = options.getExpressionFields();
    String[] oriFieldNames = source.getFieldNames();
    List<String> fieldNames = new LinkedList<>(Arrays.asList(oriFieldNames));
    List<DataWorkflowFieldType> dataWorkflowFieldTypes =
        new LinkedList<>(Arrays.asList(source.getFieldTypes()));
    // 添加字段名、字段类型
    for (ExpressionTransformConfig.ExpressionField field : fields) {
      fieldNames.add(field.getDestination());
      dataWorkflowFieldTypes.add(DataWorkflowFieldTypeParse.parseDataType(field.getType()));
    }
    List<Object[]> results = new LinkedList<>();
    Object[] values;
    while ((values = source.pollNext()) != null) {
      List<Object> objects = new LinkedList<>(Arrays.asList(values));
      for (ExpressionTransformConfig.ExpressionField field : fields) {
        Map<String, Object> env = buildExpressionEnv(oriFieldNames, objects);
        String expressionName = field.getExpression();
        Expression expression = compiledMap.get(expressionName);
        if (expression == null) {
          throw new DataWorkflowException(
              String.format("expression [%s] not find", expressionName));
        }
        Object execute = expression.execute(env);
        objects.add(execute);
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

  /**
   * 构建表达式运行变量环境
   *
   * @param fieldNames 列名
   * @param values 值
   * @return env
   */
  private Map<String, Object> buildExpressionEnv(String[] fieldNames, List<Object> values) {
    Map<String, Object> env = new HashMap<>();
    for (int i = 0; i < fieldNames.length; i++) {
      env.put(fieldNames[i], values.get(i));
    }
    return env;
  }

  @Override
  public ExpressionTransformConfig getTransformConfig() {
    return config;
  }
}
