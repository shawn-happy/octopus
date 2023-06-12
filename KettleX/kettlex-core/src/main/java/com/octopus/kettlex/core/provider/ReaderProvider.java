package com.octopus.kettlex.core.provider;

import com.octopus.kettlex.core.steps.Reader;
import com.octopus.kettlex.core.steps.config.ReaderConfig;

public interface ReaderProvider<S extends Reader<?>, C extends ReaderConfig<?>>
    extends StepProvider {

  Class<S> getReader();

  Class<C> getReaderConfig();
}
