package com.octopus.kettlex.runtime.provider;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.provider.ReaderProvider;
import com.octopus.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.kettlex.core.provider.StepConfigStepCombinationsBuilder;
import com.octopus.kettlex.core.provider.StepProviderServiceLoader;
import com.octopus.kettlex.core.provider.TransformProvider;
import com.octopus.kettlex.core.provider.WriterProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

public class DefaultStepConfigStepCombinationsBuilder implements StepConfigStepCombinationsBuilder {

  private final List<StepConfigStepCombination> stepConfigStepCombinations;
  private final List<String> stepTypes;
  private final StepProviderServiceLoader stepProviderServiceLoader =
      StepProviderServiceLoader.getInstance();
  private final ClassLoader classLoader;

  public DefaultStepConfigStepCombinationsBuilder(ClassLoader classLoader) {
    if (classLoader == null) {
      this.classLoader = Thread.currentThread().getContextClassLoader();
    } else {
      this.classLoader = classLoader;
    }
    stepConfigStepCombinations = new ArrayList<>();
    stepTypes = new ArrayList<>();
  }

  @Override
  public StepConfigStepCombinationsBuilder addBuiltInStepConfigSteps() {
    Arrays.stream(BuiltInSteps.values())
        .forEach(
            builtInStep -> {
              addStepType(builtInStep.getStepConfigStepCombination().getType());
              stepConfigStepCombinations.add(builtInStep.getStepConfigStepCombination());
            });
    return this;
  }

  @Override
  public StepConfigStepCombinationsBuilder addDiscoveredStepConfigSteps() {
    List<ReaderProvider<?, ?>> readerProviders =
        stepProviderServiceLoader.findReaderProviders(classLoader);
    if (CollectionUtils.isNotEmpty(readerProviders)) {
      for (ReaderProvider<?, ?> readerProvider : readerProviders) {
        addStepType(readerProvider.getType());
        stepConfigStepCombinations.add(
            newStepConfigStepCombination(
                readerProvider.getType(),
                readerProvider.getReader(),
                readerProvider.getReaderConfig(),
                classLoader));
      }
    }

    List<TransformProvider<?, ?>> transformationProviders =
        stepProviderServiceLoader.findTransformations(classLoader);
    if (CollectionUtils.isNotEmpty(transformationProviders)) {
      for (TransformProvider<?, ?> transformProvider : transformationProviders) {
        addStepType(transformProvider.getType());
        stepConfigStepCombinations.add(
            newStepConfigStepCombination(
                transformProvider.getType(),
                transformProvider.getTransform(),
                transformProvider.getTransformConfig(),
                classLoader));
      }
    }

    List<WriterProvider<?, ?>> writerProviders = stepProviderServiceLoader.findWriters(classLoader);
    if (CollectionUtils.isNotEmpty(writerProviders)) {
      for (WriterProvider<?, ?> writerProvider : writerProviders) {
        addStepType(writerProvider.getType());
        stepConfigStepCombinations.add(
            newStepConfigStepCombination(
                writerProvider.getType(),
                writerProvider.getWriter(),
                writerProvider.getWriterConfig(),
                classLoader));
      }
    }

    return this;
  }

  @Override
  public List<StepConfigStepCombination> build() {
    return stepConfigStepCombinations;
  }

  private StepConfigStepCombination newStepConfigStepCombination(
      String type, Class<?> step, Class<?> stepConfig, ClassLoader classLoader) {
    return new StepConfigStepCombination(type, step, stepConfig, classLoader);
  }

  private void addStepType(String type) {
    if (stepTypes.contains(type)) {
      throw new KettleXException(String.format("step type [%s] is conflict", type));
    }
    stepTypes.add(type);
  }
}
