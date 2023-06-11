package com.octopus.kettlex.runtime.registry;

import com.octopus.kettlex.core.steps.Reader;
import com.octopus.kettlex.core.steps.Transform;
import com.octopus.kettlex.core.steps.Writer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class StepRegistry {

  public List<Reader<?>> findReaders() {
    return findReaders(null);
  }

  public List<Reader<?>> findReaders(ClassLoader classLoader) {
    ServiceLoader<Reader> readerServiceLoader = secureGetServiceLoader(Reader.class, classLoader);
    List<Reader<?>> readers = new ArrayList<>();
    for (Reader<?> reader : readerServiceLoader) {
      readers.add(reader);
    }
    return readers;
  }

  public List<Transform<?>> findTransformtions() {
    return findTransformtions(null);
  }

  public List<Transform<?>> findTransformtions(ClassLoader classLoader) {
    ServiceLoader<Transform> transformServiceLoader =
        secureGetServiceLoader(Transform.class, classLoader);
    List<Transform<?>> transforms = new ArrayList<>();
    for (Transform<?> transform : transformServiceLoader) {
      transforms.add(transform);
    }
    return transforms;
  }

  public List<Writer<?>> findWriters() {
    return findWriters(null);
  }

  public List<Writer<?>> findWriters(ClassLoader classLoader) {
    ServiceLoader<Writer> writerServiceLoader = secureGetServiceLoader(Writer.class, classLoader);
    List<Writer<?>> writers = new ArrayList<>();
    for (Writer<?> writer : writerServiceLoader) {
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
        new PrivilegedAction<ServiceLoader<T>>() {
          @Override
          public ServiceLoader<T> run() {
            return (classLoader == null)
                ? ServiceLoader.load(clazz)
                : ServiceLoader.load(clazz, classLoader);
          }
        });
  }
}
