package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.model.WriterConfig;
import java.util.function.Consumer;

public abstract class BaseWriter<C extends WriterConfig<?>> extends BaseStep<C>
    implements Writer<C> {

  protected BaseWriter(C stepConfig) {
    super(stepConfig);
  }

  @Override
  public void writer() throws KettleXStepExecuteException {
    Record record;
    while ((record = getRow()) != null) {
      doWriter().accept(record);
    }
  }

  protected abstract Consumer<Record> doWriter();
}
