package io.github.shawn.octopus.fluxus.engine.common.file.serialization;

import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.connector.source.file.FileSourceConfig;
import io.github.shawn.octopus.fluxus.engine.model.type.ArrayFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CSVToRowRecordDeserializationTests {
  @Test
  public void testCSV() throws Exception {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("example/source/user.csv");
    BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                Files.newInputStream(Paths.get(new File(resource.toURI()).getAbsolutePath())),
                StandardCharsets.UTF_8));

    RowFieldType rowFieldType =
        new RowFieldType(
            new String[] {"id", "name", "array"},
            new DataWorkflowFieldType[] {
              BasicFieldType.STRING, BasicFieldType.STRING, ArrayFieldType.INT_ARRAY
            });
    FileSourceConfig.CSVFileSourceOptions csvFileSourceOptions =
        new FileSourceConfig.CSVFileSourceOptions();
    csvFileSourceOptions.setSeparator(',');
    csvFileSourceOptions.setArrayElementSeparator(";");
    CSVToRowRecordDeserialization csvToRowRecordDeserialization =
        new CSVToRowRecordDeserialization(rowFieldType, csvFileSourceOptions);
    String content = reader.readLine();
    while ((content = reader.readLine()) != null) {
      RowRecord deserialize =
          csvToRowRecordDeserialization.deserialize(content.getBytes(StandardCharsets.UTF_8));
      Assertions.assertNotNull(deserialize);
    }
    reader.close();
  }
}
