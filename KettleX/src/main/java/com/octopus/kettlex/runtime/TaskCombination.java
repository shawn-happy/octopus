package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.model.ReaderConfig;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.model.WriterConfig;
import java.util.*;
import org.apache.commons.collections4.CollectionUtils;

public class TaskCombination {

  private final TaskConfiguration configuration;
  private final List<String> outputs = new ArrayList<>();
  private final Map<String, List<String>> outputInputs = new HashMap<>();

  public TaskCombination(TaskConfiguration configuration) {
    this.configuration = configuration;
  }

  public void combi() {
    String taskId = configuration.getTaskId();
    List<ReaderConfig<?>> readers = configuration.getReaders();
    if (CollectionUtils.isEmpty(readers)) {
      throw new KettleXException("reader config cannot be null");
    }

    for (ReaderConfig<?> reader : readers) {
      String output = reader.getOutput();
      if (outputs.contains(output)) {
        throw new KettleXException(
            String.format(
                "output must be unique. outputs: [%s], current output:[%s]", outputs, output));
      }
      outputs.add(output);
    }
    List<TransformationConfig<?>> transformations = configuration.getTransformations();
    if (CollectionUtils.isNotEmpty(transformations)) {
      for (TransformationConfig<?> transformation : transformations) {
        String input = transformation.getInput();
        if (!outputs.contains(input)) {
          throw new KettleXException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputs, input));
        }
        String output = transformation.getOutput();
        if (outputs.contains(output)) {
          throw new KettleXException(
              String.format(
                  "output must be unique. outputs: [%s], current output:[%s]", outputs, output));
        }
        outputs.add(output);

        List<String> inputs = outputInputs.get(output);
        if (CollectionUtils.isEmpty(inputs)) {
          inputs = new ArrayList<>();
        }
        inputs.add(input);
        outputInputs.put(output, inputs);
      }
    }
    List<WriterConfig<?>> writers = configuration.getWriters();
    if (CollectionUtils.isNotEmpty(writers)) {
      for (WriterConfig<?> writerConfig : writers) {
        String input = writerConfig.getInput();
        if (!outputs.contains(input)) {
          throw new KettleXException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputs, input));
        }
      }
    }
  }
}
