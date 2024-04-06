package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.write;

import static com.google.common.base.Preconditions.checkState;
import static io.github.shawn.octopus.fluxus.engine.connector.sink.doris.DorisConstants.LINE_DELIMITER_DEFAULT;
import static io.github.shawn.octopus.fluxus.engine.connector.sink.doris.DorisConstants.LINE_DELIMITER_KEY;
import static io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.DorisRestUtils.LABEL_EXIST_PATTERN;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.DorisSinkConfig;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.exception.DorisConnectorException;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.DorisRestUtils;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.RestService;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model.RespContent;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

@Slf4j
public class DorisStreamLoad {
  private static final int HTTP_TEMPORARY_REDIRECT = 200;
  private final LabelGenerator labelGenerator;
  private final byte[] lineDelimiter;
  private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
  private static final String TWO_PC_URL_PATTERN = "http://%s/api/%s/_stream_load_2pc";
  private static final String JOB_EXIST_FINISHED = "FINISHED";

  private final DorisSinkConfig.DorisSinkOptions dorisConfig;

  private final String loadUrlStr;
  private String hostPort;
  private final String abortUrlStr;
  private final String user;
  private final String passwd;
  private final String db;
  private final boolean enable2PC;
  private final boolean enableDelete;
  private final Properties streamLoadProp;
  private final RecordStream recordStream;

  @Getter private Future<CloseableHttpResponse> pendingLoadFuture;
  private final CloseableHttpClient httpClient;
  private final ExecutorService executorService;
  private boolean loadBatchFirstRecord;

  public DorisStreamLoad(
      String hostPort,
      DorisSinkConfig.DorisSinkOptions dorisConfig,
      LabelGenerator labelGenerator,
      CloseableHttpClient httpClient) {
    this.dorisConfig = dorisConfig;
    this.hostPort = hostPort;
    this.db = dorisConfig.getDatabase();
    this.user = dorisConfig.getUsername();
    this.passwd = dorisConfig.getPassword();
    this.labelGenerator = labelGenerator;
    this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort, db, dorisConfig.getTable());
    this.abortUrlStr = String.format(TWO_PC_URL_PATTERN, hostPort, db);
    this.enable2PC = dorisConfig.isEnable2PC();
    this.streamLoadProp = dorisConfig.getStreamLoadProps();
    this.enableDelete = dorisConfig.isEnableDelete();
    this.httpClient = httpClient;
    this.executorService =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("stream-load-upload").build());
    this.recordStream = new RecordStream(dorisConfig.getBufferSize(), dorisConfig.getBufferCount());
    lineDelimiter =
        streamLoadProp.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT).getBytes();
    loadBatchFirstRecord = true;
  }

  public void abortPreCommit(String labelSuffix, long chkID) throws Exception {
    long startChkID = chkID;
    log.info("abort for labelSuffix {}. start chkId {}.", labelSuffix, chkID);
    while (true) {
      try {
        String label = labelGenerator.generateLabel(startChkID);
        HttpPutBuilder builder = new HttpPutBuilder();
        builder
            .setUrl(loadUrlStr)
            .baseAuth(user, passwd)
            .addCommonHeader()
            .enable2PC()
            .setLabel(label)
            .setEmptyEntity()
            .addProperties(streamLoadProp);
        RespContent respContent = handlePreCommitResponse(httpClient.execute(builder.build()));
        checkState("true".equals(respContent.getTwoPhaseCommit()));
        if (LoadStatus.LABEL_ALREADY_EXIST.equals(respContent.getStatus())) {
          // label already exist and job finished
          if (JOB_EXIST_FINISHED.equals(respContent.getExistingJobStatus())) {
            throw new DorisConnectorException(
                "Load status is "
                    + LoadStatus.LABEL_ALREADY_EXIST
                    + " and load job finished, "
                    + "change you label prefix or restore from latest savepoint!");
          }
          // job not finished, abort.
          Matcher matcher = LABEL_EXIST_PATTERN.matcher(respContent.getMessage());
          if (matcher.find()) {
            checkState(label.equals(matcher.group(1)));
            long txnId = Long.parseLong(matcher.group(2));
            log.info("abort {} for exist label {}", txnId, label);
            abortTransaction(txnId);
          } else {
            throw new DorisConnectorException(
                "Load Status is "
                    + LoadStatus.LABEL_ALREADY_EXIST
                    + ", but no txnID associated with it!"
                    + "response: "
                    + respContent);
          }
        } else {
          log.info("abort {} for check label {}.", respContent.getTxnId(), label);
          abortTransaction(respContent.getTxnId());
          break;
        }
        startChkID++;
      } catch (Exception e) {
        log.warn("failed to stream load data", e);
        throw e;
      }
    }
    log.info("abort for labelSuffix {} finished", labelSuffix);
  }

  public void writeRecord(byte[] record) throws IOException {
    if (loadBatchFirstRecord) {
      loadBatchFirstRecord = false;
    } else {
      recordStream.write(lineDelimiter);
    }
    recordStream.write(record);
  }

  public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception {
    final int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode == HTTP_TEMPORARY_REDIRECT && response.getEntity() != null) {
      String loadResult = EntityUtils.toString(response.getEntity());
      log.info("load Result {}", loadResult);
      return JsonUtils.fromJson(loadResult, new TypeReference<RespContent>() {}).orElse(null);
    }
    throw new DorisConnectorException(response.getStatusLine().toString());
  }

  public RespContent stopLoad() throws IOException {
    recordStream.endInput();
    log.info("stream load stopped.");
    checkState(pendingLoadFuture != null);
    try {
      return handlePreCommitResponse(pendingLoadFuture.get());
    } catch (Exception e) {
      throw new DorisConnectorException(e);
    }
  }

  public void startLoad(String label) {
    loadBatchFirstRecord = true;
    HttpPutBuilder putBuilder = new HttpPutBuilder();
    recordStream.startInput();
    log.info("stream load started for {}", label);
    try {
      InputStreamEntity entity = new InputStreamEntity(recordStream);
      putBuilder
          .setUrl(loadUrlStr)
          .baseAuth(user, passwd)
          .addCommonHeader()
          .addHiddenColumns(enableDelete)
          .setLabel(label)
          .setEntity(entity)
          .addProperties(streamLoadProp);
      if (enable2PC) {
        putBuilder.enable2PC();
      }
      pendingLoadFuture =
          executorService.submit(
              () -> {
                log.info("start execute load");
                return httpClient.execute(putBuilder.build());
              });
    } catch (Exception e) {
      String err = "failed to stream load data with label: " + label;
      log.warn(err, e);
      throw e;
    }
  }

  public void commitTransaction(long txnID) throws Exception {
    int statusCode;
    String reasonPhrase;
    CloseableHttpResponse response = null;
    HttpPutBuilder putBuilder = new HttpPutBuilder();
    putBuilder
        .setUrl(String.format(TWO_PC_URL_PATTERN, hostPort, db))
        .baseAuth(dorisConfig.getUsername(), dorisConfig.getPassword())
        .addCommonHeader()
        .addTxnId(txnID)
        .setEmptyEntity()
        .commit();
    try {
      response = httpClient.execute(putBuilder.build());
    } catch (IOException e) {
      log.error("commit transaction failed: ", e);
      hostPort = RestService.getBackend(dorisConfig);
    }
    if (response == null) {
      throw new DataWorkflowException("cannot commit transaction");
    }
    statusCode = response.getStatusLine().getStatusCode();
    reasonPhrase = response.getStatusLine().getReasonPhrase();
    if (statusCode != HTTP_TEMPORARY_REDIRECT) {
      log.warn("commit failed with {}, reason {}", hostPort, reasonPhrase);
      hostPort = RestService.getBackend(dorisConfig);
    }

    if (statusCode != HTTP_TEMPORARY_REDIRECT) {
      throw new DorisConnectorException(reasonPhrase);
    }

    if (response.getEntity() != null) {
      String loadResult = EntityUtils.toString(response.getEntity());
      Map<String, String> res =
          JsonUtils.fromJson(loadResult, new TypeReference<HashMap<String, String>>() {}).get();
      if (res.get("status").equals(LoadStatus.FAIL)
          && !DorisRestUtils.isCommitted(res.get("msg"))) {
        throw new DorisConnectorException(loadResult);
      } else {
        log.info("load result {}", loadResult);
      }
    }
  }

  public void abortTransaction(long txnID) throws Exception {
    HttpPutBuilder builder = new HttpPutBuilder();
    builder
        .setUrl(abortUrlStr)
        .baseAuth(user, passwd)
        .addCommonHeader()
        .addTxnId(txnID)
        .setEmptyEntity()
        .abort();
    CloseableHttpResponse response = httpClient.execute(builder.build());

    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != HTTP_TEMPORARY_REDIRECT || response.getEntity() == null) {
      log.warn("abort transaction response: " + response.getStatusLine().toString());
      throw new DorisConnectorException(
          "Fail to abort transaction " + txnID + " with url " + abortUrlStr);
    }

    String loadResult = EntityUtils.toString(response.getEntity());
    Map<String, String> res =
        JsonUtils.fromJson(loadResult, new TypeReference<HashMap<String, String>>() {}).get();
    if (!LoadStatus.SUCCESS.equals(res.get("status"))) {
      if (DorisRestUtils.isCommitted(res.get("msg"))) {
        throw new DorisConnectorException(
            "try abort committed transaction, " + "do you recover from old savepoint?");
      }
      log.warn("Fail to abort transaction. txnId: {}, error: {}", txnID, res.get("msg"));
    }
  }

  public void close() throws IOException {
    if (null != httpClient) {
      try {
        httpClient.close();
      } catch (IOException e) {
        throw new IOException("Closing httpClient failed.", e);
      }
    }
    if (null != executorService) {
      executorService.shutdownNow();
    }
  }
}
