package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.model.ReaderConfig;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.collections4.CollectionUtils;

public abstract class BaseReader<C extends ReaderConfig<?>> extends BaseStep<C>
    implements Reader<C> {

  protected BaseReader(C stepConfig) {
    super(stepConfig);
  }

  @Override
  public void read() throws KettleXStepExecuteException {
    List<Record> records = doReader().get();
    if (CollectionUtils.isEmpty(records)) {
      return;
    }
    for (Record record : records) {
      putRow(record);
    }
  }

  protected abstract Supplier<List<Record>> doReader();
}
