package com.octopus.operators.flink.runtime;

import static com.octopus.operators.flink.runtime.FlinkRuntimeEnvironmentConstant.CHECKPOINT_CLEANUP_MODE;
import static com.octopus.operators.flink.runtime.FlinkRuntimeEnvironmentConstant.CHECKPOINT_DATA_URI;
import static com.octopus.operators.flink.runtime.FlinkRuntimeEnvironmentConstant.CHECKPOINT_MODE;
import static com.octopus.operators.flink.runtime.FlinkRuntimeEnvironmentConstant.FAIL_ON_CHECKPOINTING_ERRORS;
import static com.octopus.operators.flink.runtime.FlinkRuntimeEnvironmentConstant.MAX_CONCURRENT_CHECKPOINTS;
import static com.octopus.operators.flink.runtime.FlinkRuntimeEnvironmentConstant.MIN_PAUSE_BETWEEN_CHECKPOINTS;
import static com.octopus.operators.flink.runtime.FlinkRuntimeEnvironmentConstant.TIME_CHARACTERISTIC;

import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.config.EngineType;
import com.octopus.operators.engine.config.RuntimeEnvironment;
import com.octopus.operators.engine.config.TaskConfig;
import com.octopus.operators.engine.config.TaskMode;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Slf4j
public class FlinkRuntimeEnvironment implements RuntimeEnvironment {
  @Getter private final EngineType engine = EngineType.FLINK;
  @Setter @Getter private TaskConfig taskConfig;
  private StreamExecutionEnvironment environment;
  private StreamTableEnvironment tableEnvironment;
  private String taskName = "flink-job";

  @Override
  public CheckResult checkTaskConfig() {
    return null;
  }

  @Override
  public RuntimeEnvironment prepare() {
    createStreamEnvironment();
    createStreamTableEnvironment();
    return this;
  }

  public StreamExecutionEnvironment getStreamExecutionEnvironment() {
    return environment;
  }

  public StreamTableEnvironment getStreamTableEnvironment() {
    return tableEnvironment;
  }

  private void createStreamTableEnvironment() {
    EnvironmentSettings environmentSettings =
        EnvironmentSettings.newInstance().inStreamingMode().build();
    tableEnvironment =
        StreamTableEnvironment.create(getStreamExecutionEnvironment(), environmentSettings);
  }

  private void createStreamEnvironment() {
    Configuration configuration = new Configuration();
    environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    setTimeCharacteristic();

    setCheckpoint();
    TaskMode taskMode = taskConfig.getTaskMode();
    switch (taskMode) {
      case BATCH:
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        break;
      case STREAMING:
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        break;
    }
    environment.setParallelism(taskConfig.getParallelism());
  }

  private void setTimeCharacteristic() {
    Map<String, Object> runtimeConfig = taskConfig.getRuntimeConfig();
    if (runtimeConfig.containsKey(TIME_CHARACTERISTIC)) {
      String timeType = runtimeConfig.get(TIME_CHARACTERISTIC).toString();
      switch (timeType.toLowerCase()) {
        case "event-time":
          environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
          break;
        case "ingestion-time":
          environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
          break;
        case "processing-time":
          environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
          break;
        default:
          log.warn(
              "set time-characteristic failed, unknown time-characteristic [{}],only support event-time,ingestion-time,processing-time",
              timeType);
          break;
      }
    }
  }

  private void setCheckpoint() {
    long interval = taskConfig.getCheckpointInterval();

    if (interval > 0) {
      CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
      environment.enableCheckpointing(interval);
      Map<String, Object> runtimeConfig = taskConfig.getRuntimeConfig();
      if (runtimeConfig.containsKey(CHECKPOINT_MODE)) {
        String mode = runtimeConfig.get(CHECKPOINT_MODE).toString();
        switch (mode.toLowerCase()) {
          case "exactly-once":
            checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            break;
          case "at-least-once":
            checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            break;
          default:
            log.warn(
                "set checkpoint.mode failed, unknown checkpoint.mode [{}],only support exactly-once,at-least-once",
                mode);
            break;
        }
      }

      checkpointConfig.setCheckpointTimeout(taskConfig.getCheckpointTimeout());

      if (runtimeConfig.containsKey(CHECKPOINT_DATA_URI)) {
        String uri = runtimeConfig.get(CHECKPOINT_DATA_URI).toString();
        StateBackend fsStateBackend = new FsStateBackend(uri);
        environment.setStateBackend(fsStateBackend);
      }

      if (runtimeConfig.containsKey(MAX_CONCURRENT_CHECKPOINTS)) {
        int max = (int) runtimeConfig.get(MAX_CONCURRENT_CHECKPOINTS);
        checkpointConfig.setMaxConcurrentCheckpoints(max);
      }

      if (runtimeConfig.containsKey(CHECKPOINT_CLEANUP_MODE)) {
        boolean cleanup = (boolean) runtimeConfig.get(CHECKPOINT_CLEANUP_MODE);
        if (cleanup) {
          checkpointConfig.enableExternalizedCheckpoints(
              CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        } else {
          checkpointConfig.enableExternalizedCheckpoints(
              CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
      }

      if (runtimeConfig.containsKey(MIN_PAUSE_BETWEEN_CHECKPOINTS)) {
        long minPause = (long) runtimeConfig.get(MIN_PAUSE_BETWEEN_CHECKPOINTS);
        checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
      }

      if (runtimeConfig.containsKey(FAIL_ON_CHECKPOINTING_ERRORS)) {
        int failNum = (int) runtimeConfig.get(FAIL_ON_CHECKPOINTING_ERRORS);
        checkpointConfig.setTolerableCheckpointFailureNumber(failNum);
      }
    }
  }
}
