package com.octopus.operators.kettlex.core.provider;

import com.octopus.operators.kettlex.core.steps.Reader;
import com.octopus.operators.kettlex.core.steps.config.ReaderConfig;

public interface ReaderProvider<S extends Reader<?>, C extends ReaderConfig<?>>
    extends StepProvider {

  Class<S> getReader();

  Class<C> getReaderConfig();
}
