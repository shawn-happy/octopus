package io.github.shawn.octopus.fluxus.api.provider;

import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

public class PluginRegistry {
  private static final Map<String, SourceProvider<?, ?>> SOURCES = new HashMap<>(2 << 4);
  private static final Map<String, TransformProvider<?>> TRANSFORMS = new HashMap<>(2 << 4);
  private static final Map<String, SinkProvider<?>> SINKS = new HashMap<>(2 << 4);

  static {
    ServiceLoader<SourceProvider> sourceProviders = ServiceLoader.load(SourceProvider.class);
    for (SourceProvider<?, ?> next : sourceProviders) {
      SOURCES.put(next.getIdentifier(), next);
    }

    ServiceLoader<TransformProvider> transformProviders =
        ServiceLoader.load(TransformProvider.class);
    for (TransformProvider<?> next : transformProviders) {
      TRANSFORMS.put(next.getIdentifier(), next);
    }

    ServiceLoader<SinkProvider> sinkProviders = ServiceLoader.load(SinkProvider.class);
    for (SinkProvider<?> next : sinkProviders) {
      SINKS.put(next.getIdentifier(), next);
    }
  }

  public static Optional<SourceProvider<? extends SourceConfig<?>, ?>> getSourceProvider(
      String type) {
    return Optional.ofNullable(SOURCES.get(type));
  }

  public static Optional<TransformProvider<?>> getTransformProvider(String type) {
    return Optional.ofNullable(TRANSFORMS.get(type));
  }

  public static Optional<SinkProvider<?>> getSinkProvider(String type) {
    return Optional.ofNullable(SINKS.get(type));
  }
}
