package io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse.JsonParseTransformConfig.JsonParseField;
import io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse.JsonParseTransformConfig.JsonParseTransformOptions;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;

public class JsonParseTransform implements Transform<JsonParseTransformConfig> {

  private final JsonParseTransformConfig config;
  private final JsonParseTransformOptions options;
  private JsonPath[] jsonPaths;
  private Configuration jsonConfiguration;
  private ParseContext parseContext;

  public JsonParseTransform(JsonParseTransformConfig config) {
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public void init() throws StepExecutionException {
    setDefaultJsonConfiguration();
    JsonParseField[] jsonParseFields = options.getJsonParseFields();
    jsonPaths = new JsonPath[jsonParseFields.length];
    for (int i = 0; i < jsonParseFields.length; i++) {
      jsonPaths[i] = JsonPath.compile(jsonParseFields[i].getSourcePath());
    }
    parseContext = JsonPath.using(jsonConfiguration);
  }

  @Override
  public RowRecord transform(RowRecord source) throws StepExecutionException {
    int index = source.indexOf(options.getValueField());
    List<Object[]> results = new LinkedList<>();
    while (true) {
      Object[] values = source.pollNext();
      if (ArrayUtils.isEmpty(values)) {
        break;
      }
      Object value = values[index];
      List<List<?>> combinedResult = evalCombinedResult(value);
      List<Object[]> record = buildRowDataList(combinedResult);
      results.addAll(record);
    }
    TransformRowRecord rowRecord =
        new TransformRowRecord(options.getDestinations(), options.getDestinationTypes());
    rowRecord.addRecords(results);
    return rowRecord;
  }

  @Override
  public JsonParseTransformConfig getTransformConfig() {
    return config;
  }

  private void setDefaultJsonConfiguration() {
    this.jsonConfiguration =
        Configuration.defaultConfiguration()
            .addOptions(
                Option.ALWAYS_RETURN_LIST,
                Option.SUPPRESS_EXCEPTIONS,
                Option.DEFAULT_PATH_LEAF_TO_NULL)
            .jsonProvider(new JacksonJsonProvider());
  }

  private List<List<?>> evalCombinedResult(Object value) {
    List<List<?>> results = new ArrayList<>(jsonPaths.length);
    int lastSize = -1;
    int maxSize = 0;
    String prevPath = null;
    ReadContext readContext = parseContext.parse(String.valueOf(value));

    for (JsonPath path : jsonPaths) {
      List<?> result = readContext.read(path);
      if (result.size() != lastSize && lastSize > 0 && !result.isEmpty()) {
        throw new StepExecutionException(
            String.format(
                "The data structure is not the same inside the resource\\! We found %s values for json path [%s], which is different that the number returned for path [%s] (%s values). We MUST have the same number of values for all paths.",
                result.size(), path.getPath(), prevPath, lastSize));
      }
      results.add(result);
      lastSize = result.size();
      maxSize = Math.max(maxSize, lastSize);
      prevPath = path.getPath();
    }
    return results;
  }

  private List<Object[]> buildRowDataList(List<List<?>> results) {
    List<Object[]> result = new ArrayList<>();
    Object[] rowData = null;
    int rowNum = results.get(0).size(); // 获取记录条数
    for (int i = 0; i < rowNum; i++) {
      rowData = new Object[results.size()];
      result.add(rowData);
      for (int j = 0; j < results.size(); j++) {
        rowData[j] = results.get(j).get(i);
      }
    }
    return result;
  }
}
