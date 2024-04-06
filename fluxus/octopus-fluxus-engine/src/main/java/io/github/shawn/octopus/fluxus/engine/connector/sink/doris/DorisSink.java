package io.github.shawn.octopus.fluxus.engine.connector.sink.doris;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.HttpUtil;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.RestService;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model.BackendV2;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model.RespContent;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.serialize.DorisCodec;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.serialize.DorisCodecFactory;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.write.DorisStreamLoad;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.write.LabelGenerator;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.write.LoadStatus;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContextManagement;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DorisSink implements Sink<DorisSinkConfig> {

  private static final int INITIAL_DELAY = 200;
  private static final int CONNECT_TIMEOUT = 1000;
  private static final List<String> DORIS_SUCCESS_STATUS =
      new ArrayList<>(Arrays.asList(LoadStatus.SUCCESS, LoadStatus.PUBLISH_TIMEOUT));

  private final DorisSinkConfig config;
  private final DorisSinkConfig.DorisSinkOptions options;
  private volatile boolean loading;

  private int pos;
  private long lastCheckpointId;
  private List<BackendV2.BackendRowV2> backends;
  private DorisStreamLoad dorisStreamLoad;
  private DorisCodec codec;

  private final String labelPrefix;
  private final LabelGenerator labelGenerator;

  private final transient ScheduledExecutorService scheduledExecutorService;
  private transient Thread executorThread;
  private transient volatile Exception loadException = null;

  public DorisSink(DorisSinkConfig config) {
    this.config = config;
    this.options = config.getOptions();

    this.labelPrefix =
        options.getLabelPrefix()
            + "_"
            + JobContextManagement.getJob().getJobConfig().getJobId()
            + "_"
            + config.getId();
    this.labelGenerator = new LabelGenerator(labelPrefix, options.isEnable2PC());
    this.scheduledExecutorService =
        new ScheduledThreadPoolExecutor(
            1, new ThreadFactoryBuilder().setNameFormat("stream-load-check").build());
  }

  @Override
  public void init() throws StepExecutionException {
    try {
      this.backends = RestService.getBackendsV2(options);
      String backend = getAvailableBackend();
      this.dorisStreamLoad =
          new DorisStreamLoad(backend, options, labelGenerator, new HttpUtil().getHttpClient());
      if (options.isEnable2PC()) {
        dorisStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId + 1);
      }
      // get main work thread.
      executorThread = Thread.currentThread();
      dorisStreamLoad.startLoad(labelGenerator.generateLabel(lastCheckpointId + 1));
      // when uploading data in streaming mode, we need to regularly detect whether there are
      // exceptions.
      scheduledExecutorService.scheduleWithFixedDelay(
          this::checkDone, INITIAL_DELAY, options.getFlushInterval(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error("doris sink {} init error", config.getId(), e);
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void write(RowRecord source) throws StepExecutionException {
    checkLoadException();
    try {
      Object[] record;
      while ((record = source.pollNext()) != null) {
        if (record.length != options.getColumns().length) {
          throw new StepExecutionException(
              String.format(
                  "There is an error in the column configuration information. "
                      + "This is because you have configured a task where the number of fields to be read from the source:%s "
                      + "is not equal to the number of fields to be written to the destination table:%s. "
                      + "Please check your configuration and make changes.",
                  record.length, options.getColumns().length));
        }
        if (codec == null) {
          codec =
              DorisCodecFactory.createCodec(
                  options, source.getFieldNames(), source.getFieldTypes());
        }
        dorisStreamLoad.writeRecord(codec.codec(record).getBytes(StandardCharsets.UTF_8));
      }

    } catch (Exception e) {
      log.error("doris sink {} write error", config.getId(), e);
      throw new StepExecutionException(e);
    }
  }

  @Override
  public DorisSinkConfig getSinkConfig() {
    return config;
  }

  @Override
  public boolean commit() throws StepExecutionException {
    Long txnId = null;
    loading = false;
    checkState(dorisStreamLoad != null);
    try {
      RespContent respContent = dorisStreamLoad.stopLoad();
      if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
        String errMsg =
            String.format(
                "stream load error: %s, see more in %s",
                respContent.getMessage(), respContent.getErrorURL());
        throw new StepExecutionException(errMsg);
      }
      if (!options.isEnable2PC()) {
        return true;
      }
      txnId = respContent.getTxnId();
      dorisStreamLoad.commitTransaction(txnId);
      lastCheckpointId++;
    } catch (Exception e) {
      if (txnId == null) {
        log.warn("cannot found txnId");
        return false;
      }
      int retry = 0;
      Exception tmp = null;
      while (retry <= 3) {
        try {
          dorisStreamLoad.abortTransaction(txnId);
          break;
        } catch (Exception ex) {
          tmp = ex;
          retry++;
        }
      }
      throw new StepExecutionException(tmp);
    }
    return true;
  }

  @Override
  public void abort() {
    Long txnId = null;
    loading = false;
    checkState(dorisStreamLoad != null);
    try {
      RespContent respContent = dorisStreamLoad.stopLoad();
      if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
        String errMsg =
            String.format(
                "stream load error: %s, see more in %s",
                respContent.getMessage(), respContent.getErrorURL());
        throw new StepExecutionException(errMsg);
      }
      if (!options.isEnable2PC()) {
        return;
      }
      txnId = respContent.getTxnId();
      dorisStreamLoad.abortTransaction(txnId);
    } catch (Exception e) {
      if (txnId == null) {
        log.warn("cannot found txnId");
        return;
      }
      int retry = 0;
      Exception tmp = null;
      while (retry <= 3) {
        try {
          dorisStreamLoad.abortTransaction(txnId);
          break;
        } catch (Exception ex) {
          tmp = ex;
          retry++;
        }
      }
      throw new StepExecutionException(tmp);
    }
  }

  @Override
  public void dispose() throws StepExecutionException {
    try {
      if (scheduledExecutorService != null) {
        scheduledExecutorService.shutdownNow();
      }
      if (dorisStreamLoad != null) {
        dorisStreamLoad.close();
      }
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  private String getAvailableBackend() {
    long tmp = pos + backends.size();
    while (pos < tmp) {
      BackendV2.BackendRowV2 backend = backends.get(pos % backends.size());
      String res = backend.toBackendString();
      if (tryHttpConnection(res)) {
        pos++;
        return res;
      }
    }
    String errMsg = "no available backend.";
    throw new StepExecutionException(errMsg);
  }

  public boolean tryHttpConnection(String backend) {
    try {
      backend = "http://" + backend;
      URL url = new URL(backend);
      HttpURLConnection co = (HttpURLConnection) url.openConnection();
      co.setConnectTimeout(CONNECT_TIMEOUT);
      co.connect();
      co.disconnect();
      return true;
    } catch (Exception ex) {
      log.warn("Failed to connect to backend:{}", backend, ex);
      pos++;
      return false;
    }
  }

  private void checkDone() {
    // the load future is done and checked in prepareCommit().
    // this will check error while loading.
    log.debug("start timer checker, interval {} ms", options.getFlushInterval());
    if (dorisStreamLoad.getPendingLoadFuture() != null
        && dorisStreamLoad.getPendingLoadFuture().isDone()) {
      if (!loading) {
        log.debug("not loading, skip timer checker");
        return;
      }
      String errorMsg;
      try {
        RespContent content =
            dorisStreamLoad.handlePreCommitResponse(dorisStreamLoad.getPendingLoadFuture().get());
        errorMsg = content.getMessage();
      } catch (Exception e) {
        errorMsg = e.getMessage();
      }

      loadException = new StepExecutionException(errorMsg);
      log.error("stream load finished unexpectedly, interrupt worker thread! {}", errorMsg);
      // set the executor thread interrupted in case blocking in write data.
      executorThread.interrupt();
    }
  }

  private void checkLoadException() {
    if (loadException != null) {
      throw new StepExecutionException("error while loading data.", loadException);
    }
  }
}
