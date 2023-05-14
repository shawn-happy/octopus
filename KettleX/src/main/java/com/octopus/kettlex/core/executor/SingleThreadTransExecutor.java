package com.octopus.kettlex.core.executor;

import com.octopus.kettlex.core.statistics.ExecutionState;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.trans.TransMeta;
import java.util.Collections;
import java.util.Comparator;

public class SingleThreadTransExecutor extends AbstractTransExecutor {

  public SingleThreadTransExecutor(TransMeta transMeta) {
    super(transMeta);
  }

  @Override
  protected void startExecute() {
    new Thread(
            () -> {
              Collections.sort(
                  steps,
                  new Comparator<Step>() {
                    @Override
                    public int compare(Step c1, Step c2) {
                      Integer seq1 = (c1.getLevelSeq() == null) ? 0 : c1.getLevelSeq();
                      Integer seq2 = (c2.getLevelSeq() == null) ? 0 : c2.getLevelSeq();
                      return seq1.compareTo(seq2);
                    }
                  });
              try {
                boolean[] stepDone = new boolean[steps.size()];
                int nrDone = 0;
                while (nrDone < steps.size() && !isStopped()) {
                  for (int i = 0; i < steps.size() && !isStopped(); i++) {
                    Step step = steps.get(i);
                    if (!stepDone[i]) {
                      boolean cont = step.processRow();
                      if (!cont) {
                        stepDone[i] = true;
                        nrDone++;
                      }
                    }
                  }
                }
                markStatus(ExecutionState.SUCCEEDED);
                for (Step step : steps) {
                  step.markStatus(ExecutionState.SUCCEEDED);
                }
              } catch (Exception e) {
                markStatus(ExecutionState.FAILED);
                for (Step step : steps) {
                  step.markStatus(ExecutionState.FAILED);
                }
              } finally {
                for (Step step : steps) {
                  step.dispose();
                }
              }
            })
        .start();
  }
}
