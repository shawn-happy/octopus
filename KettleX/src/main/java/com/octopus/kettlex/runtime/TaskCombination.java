package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.model.ReaderConfig;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.model.WriterConfig;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;

public class TaskCombination {

  private final TaskConfiguration configuration;

  public TaskCombination(TaskConfiguration configuration) {
    this.configuration = configuration;
  }

  public void combi() {
    String taskId = configuration.getTaskId();
    List<ReaderConfig<?>> readers = configuration.getReaders();
    if (CollectionUtils.isEmpty(readers)) {
      throw new KettleXException("reader config cannot be null");
    }

    Set<String> outputs = new LinkedHashSet<>();
    for (ReaderConfig<?> reader : readers) {
      String output = reader.getOutput();
      outputs.add(output);
    }
    List<TransformationConfig<?>> transformations = configuration.getTransformations();
    if (CollectionUtils.isNotEmpty(transformations)) {
      for (TransformationConfig<?> transformation : transformations) {
        String input = transformation.getInput();
        outputs.contains(input);
        String output = transformation.getOutput();
        outputs.add(output);
      }
    }
    List<WriterConfig<?>> writers = configuration.getWriters();
    if (CollectionUtils.isNotEmpty(writers)) {
      for (WriterConfig<?> writerConfig : writers) {
        String input = writerConfig.getInput();
        outputs.contains(input);
      }
    }
  }
}
