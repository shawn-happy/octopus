package io.github.shawn.octopus.fluxus.engine.connector.source.file;

import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class HdfsFileSourceTests {
  @Test
  public void testLocalFileSourceJson() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/source/hdfs-file.json")),
            StandardCharsets.UTF_8);
    FileSourceConfig sourceConfig = new FileSourceConfig();
    FileSourceConfig fileSourceConfig = (FileSourceConfig) sourceConfig.toSourceConfig(json);
    FileSource fileSource = new FileSource(fileSourceConfig);
    fileSource.init();
    RowRecord record;
    while ((record = fileSource.read()) != null) {
      System.out.println(Arrays.toString(record.pollNext()));
    }
    fileSource.dispose();
  }
}
