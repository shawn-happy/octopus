package com.octopus.kettlex.core.provider;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class StepProviderServiceLoader {

  private StepProviderServiceLoader() {}

  private static final StepProviderServiceLoader INSTANCE = new StepProviderServiceLoader();

  public static StepProviderServiceLoader getInstance() {
    return INSTANCE;
  }

  public List<ReaderProvider<?, ?>> findReaderProviders() {
    return findReaderProviders(null);
  }

  public List<ReaderProvider<?, ?>> findReaderProviders(ClassLoader classLoader) {
    ServiceLoader<ReaderProvider> readerServiceLoader =
        secureGetServiceLoader(ReaderProvider.class, classLoader);
    List<ReaderProvider<?, ?>> readerProviders = new ArrayList<>();
    for (ReaderProvider<?, ?> readerProvider : readerServiceLoader) {
      readerProviders.add(readerProvider);
    }
    return readerProviders;
  }

  public List<TransformProvider<?, ?>> findTransformations() {
    return findTransformations(null);
  }

  public List<TransformProvider<?, ?>> findTransformations(ClassLoader classLoader) {
    ServiceLoader<TransformProvider> transformServiceLoader =
        secureGetServiceLoader(TransformProvider.class, classLoader);
    List<TransformProvider<?, ?>> transforms = new ArrayList<>();
    for (TransformProvider<?, ?> transform : transformServiceLoader) {
      transforms.add(transform);
    }
    return transforms;
  }

  public List<WriterProvider<?, ?>> findWriters() {
    return findWriters(null);
  }

  public List<WriterProvider<?, ?>> findWriters(ClassLoader classLoader) {
    ServiceLoader<WriterProvider> writerServiceLoader =
        secureGetServiceLoader(WriterProvider.class, classLoader);
    List<WriterProvider<?, ?>> writers = new ArrayList<>();
    for (WriterProvider<?, ?> writer : writerServiceLoader) {
      writers.add(writer);
    }
    return writers;
  }

  private static <T> ServiceLoader<T> secureGetServiceLoader(
      final Class<T> clazz, final ClassLoader classLoader) {
    final SecurityManager sm = System.getSecurityManager();
    if (sm == null) {
      return (classLoader == null)
          ? ServiceLoader.load(clazz)
          : ServiceLoader.load(clazz, classLoader);
    }
    return AccessController.doPrivileged(
        (PrivilegedAction<ServiceLoader<T>>)
            () ->
                (classLoader == null)
                    ? ServiceLoader.load(clazz)
                    : ServiceLoader.load(clazz, classLoader));
  }
}
