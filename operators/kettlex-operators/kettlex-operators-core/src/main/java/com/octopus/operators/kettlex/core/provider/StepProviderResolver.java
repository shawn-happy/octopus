package com.octopus.operators.kettlex.core.provider;

import java.util.Iterator;
import java.util.ServiceLoader;

public abstract class StepProviderResolver {

  protected StepProviderResolver() {}

  private static volatile StepProviderResolver instance = null;

  public static StepProviderResolver getInstance() {
    if (instance == null) {
      synchronized (StepProviderResolver.class) {
        if (instance != null) {
          return instance;
        }
        instance = loadSpi(StepProviderResolver.class.getClassLoader());
      }
    }

    return instance;
  }

  private static StepProviderResolver loadSpi(ClassLoader classLoader) {
    ServiceLoader<StepProviderResolver> sl =
        ServiceLoader.load(StepProviderResolver.class, classLoader);
    final Iterator<StepProviderResolver> iterator = sl.iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    }
    throw new IllegalStateException("No StepProviderResolver implementation found!");
  }

  public abstract StepConfigStepCombination getStepConfigStepCombination(String type);

  public abstract StepConfigStepCombinationsBuilder getBuilder();
}
