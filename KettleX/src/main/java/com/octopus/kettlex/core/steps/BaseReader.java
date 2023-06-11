package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.model.ReaderConfig;
import com.octopus.kettlex.runtime.StepConfigChannelCombination;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.collections4.CollectionUtils;

public abstract class BaseReader<C extends ReaderConfig<?>> extends BaseStep<C>
    implements Reader<C> {

  protected BaseReader(StepConfigChannelCombination combination) {
    super(combination);
  }

  @Override
  public void read() throws KettleXStepExecuteException {
    if (isShutdown()) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
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
