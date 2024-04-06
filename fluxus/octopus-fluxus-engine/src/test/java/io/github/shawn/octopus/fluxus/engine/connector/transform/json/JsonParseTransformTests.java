package io.github.shawn.octopus.fluxus.engine.connector.transform.json;

import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse.JsonParseTransform;
import io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse.JsonParseTransformConfig;
import io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse.JsonParseTransformConfig.JsonParseField;
import io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse.JsonParseTransformConfig.JsonParseTransformOptions;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class JsonParseTransformTests {
  @Test
  public void test() throws Exception {
    JsonParseTransformConfig config =
        JsonParseTransformConfig.builder()
            .options(
                JsonParseTransformOptions.builder()
                    .valueField("content")
                    .jsonParseFields(
                        new JsonParseField[] {
                          JsonParseField.builder()
                              .destination("bicycle")
                              .sourcePath("$.store.bicycle.*")
                              .type("String")
                              .build(),
                          JsonParseField.builder()
                              .destination("expensive")
                              .sourcePath("$.expensive")
                              .type("String")
                              .build(),
                          JsonParseField.builder()
                              .destination("books")
                              .sourcePath("$.store.book.*")
                              .type("String")
                              .build()
                        })
                    .build())
            .build();
    SourceRowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"content"})
            .fieldTypes(new DataWorkflowFieldType[] {BasicFieldType.STRING})
            .values(
                new Object[] {
                  Resources.toString(
                      Objects.requireNonNull(
                          Thread.currentThread()
                              .getContextClassLoader()
                              .getResource("example/transform/json/data.json")),
                      StandardCharsets.UTF_8)
                })
            .build();
    JsonParseTransform jsonParseTransform = new JsonParseTransform(config);
    jsonParseTransform.init();
    jsonParseTransform.begin();
    RowRecord transform = jsonParseTransform.transform(record);
    System.out.println(transform);
  }
}
