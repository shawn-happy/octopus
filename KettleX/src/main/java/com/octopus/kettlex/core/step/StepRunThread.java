package com.octopus.kettlex.core.step;

import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepContext;
import com.octopus.kettlex.core.steps.StepMeta;
import com.octopus.kettlex.core.steps.StepMetaContextCombination;

public class StepRunThread<SM extends StepMeta, SC extends StepContext> implements Runnable {

  private final Step<SM, SC> step;
  private final SM meta;
  private final SC context;

  public StepRunThread(StepMetaContextCombination<SM, SC> combination) {
    this.step = combination.getStep();
    this.meta = combination.getStepMeta();
    this.context = combination.getStepContext();
  }

  @Override
  public void run() {
    try {
      step.setRunning(true);
      // Wait
      while (step.processRow(meta, context)) {
        if (step.isStopped()) {
          break;
        }
      }
    } catch (Throwable t) {
      try {
        // check for OOME
        //        if ( t instanceof OutOfMemoryError ) {
        //          // Handle this different with as less overhead as possible to get an error
        // message in the log.
        //          // Otherwise it crashes likely with another OOME in Me$$ages.getString() and
        // does not log
        //          // nor call the setErrors() and stopAll() below.
        //          log.logError( "UnexpectedError: ", t );
        //        } else {
        //          t.printStackTrace();
        //          log.logError( BaseMessages.getString( "System.Log.UnexpectedError" ), t );
        //        }
        //
        //        String logChannelId = log.getLogChannelId();
        //        LoggingObjectInterface loggingObject =
        // LoggingRegistry.getInstance().getLoggingObject( logChannelId );
        //        String parentLogChannelId = loggingObject.getParent().getLogChannelId();
        //        List<String> logChannelChildren =
        // LoggingRegistry.getInstance().getLogChannelChildren( parentLogChannelId );
        //        int childIndex = Const.indexOfString( log.getLogChannelId(), logChannelChildren );
        //        if ( log.isDebug() ) {
        //          log.logDebug( "child index = " + childIndex + ", logging object : " +
        // loggingObject.toString() + " parent=" + parentLogChannelId );
        //        }
        //        KettleLogStore.getAppender().getBuffer( "2bcc6b3f-c660-4a8b-8b17-89e8cbd5b29b",
        // false );
        // baseStep.logError(Const.getStackTracker(t));
      } catch (OutOfMemoryError e) {
        e.printStackTrace();
      } finally {
        //        step.setErrors( 1 );
        step.stopAll();
      }
    } finally {
      step.dispose(meta, context);
      //      step.getLogChannel().snap( Metrics.METRIC_STEP_EXECUTION_STOP );
      //      try {
      //        long li = step.getLinesInput();
      //        long lo = step.getLinesOutput();
      //        long lr = step.getLinesRead();
      //        long lw = step.getLinesWritten();
      //        long lu = step.getLinesUpdated();
      //        long lj = step.getLinesRejected();
      //        long e = step.getErrors();
      //        if ( li > 0 || lo > 0 || lr > 0 || lw > 0 || lu > 0 || lj > 0 || e > 0 ) {
      //          log.logBasic( BaseMessages.getString( PKG, "BaseStep.Log.SummaryInfo",
      // String.valueOf( li ),
      //              String.valueOf( lo ), String.valueOf( lr ), String.valueOf( lw ),
      //              String.valueOf( lu ), String.valueOf( e + lj ) ) );
      //        } else {
      //          log.logDetailed( BaseMessages.getString( PKG, "BaseStep.Log.SummaryInfo",
      // String.valueOf( li ),
      //              String.valueOf( lo ), String.valueOf( lr ), String.valueOf( lw ),
      //              String.valueOf( lu ), String.valueOf( e + lj ) ) );
      //        }
      //      } catch ( Throwable t ) {
      //        //
      //        // it's likely an OOME, so we don't want to introduce overhead by using
      // BaseMessages.getString(), see above
      //        //
      //        log.logError( "UnexpectedError: " + Const.getStackTracker( t ) );
      //      } finally {
      //        step.markStop();
      //      }
    }
  }
}
