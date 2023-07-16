package com.octopus.operators.kettlex.core.provider;

import com.octopus.operators.kettlex.core.steps.Writer;
import com.octopus.operators.kettlex.core.steps.config.WriterConfig;

public interface WriterProvider<S extends Writer<?>, C extends WriterConfig<?>>
    extends StepProvider {

  Class<S> getWriter();

  Class<C> getWriterConfig();
}
