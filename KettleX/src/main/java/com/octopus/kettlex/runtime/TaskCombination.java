package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.channel.DefaultChannel;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.model.ReaderConfig;
import com.octopus.kettlex.model.TaskConfiguration;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.model.WriterConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;

public class TaskCombination {

  private final TaskConfiguration configuration;
  private final List<String> outputs = new ArrayList<>();
  private final Map<String, String> outputIdMap = new HashMap<>();
  private final List<StepLink> links = new ArrayList<>();

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
      outputIdMap.put(output, reader.getId());
    }
    List<TransformationConfig<?>> transformations = configuration.getTransformations();
    if (CollectionUtils.isNotEmpty(transformations)) {
      for (TransformationConfig<?> transformation : transformations) {
        String output = transformation.getOutput();
        if (outputs.contains(output)) {
          throw new KettleXException(
              String.format(
                  "output must be unique. outputs: [%s], current output:[%s]", outputs, output));
        }
        outputs.add(output);
        outputIdMap.put(output, transformation.getId());
      }

      for (TransformationConfig<?> transformation : transformations) {
        String input = transformation.getInput();
        if (!outputs.contains(input)) {
          throw new KettleXException(
              String.format(
                  "cannot find this input from outputs, outputs: [%s], current input:[%s]",
                  outputs, input));
        }
        String from = outputIdMap.get(input);
        StepLink stepLink =
            StepLink.builder()
                .from(from)
                .to(input)
                .channel(
                    new DefaultChannel(
                        configuration.getRuntimeConfig().getChannelCapcacity(),
                        getChannelId(from, input)))
                .build();
        links.add(stepLink);
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
        String from = outputIdMap.get(input);
        StepLink stepLink =
            StepLink.builder()
                .from(from)
                .to(input)
                .channel(
                    new DefaultChannel(
                        configuration.getRuntimeConfig().getChannelCapcacity(),
                        getChannelId(from, input)))
                .build();
        links.add(stepLink);
      }
    }
  }

  private String getChannelId(String from, String to) {
    return String.format("%s->%s", from, to);
  }
}
