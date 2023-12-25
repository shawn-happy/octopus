package com.octopus.operators.flink.runtime;

public interface FlinkRuntimeEnvironmentConstant {

  String TIME_CHARACTERISTIC = "execution.time-characteristic";

  String BUFFER_TIMEOUT_MILLIS = "execution.buffer.timeout";
  String PARALLELISM = "execution.parallelism";
  String MAX_PARALLELISM = "execution.max-parallelism";

  String CHECKPOINT_MODE = "execution.checkpoint.mode";
  String CHECKPOINT_TIMEOUT = "execution.checkpoint.timeout";
  String CHECKPOINT_DATA_URI = "execution.checkpoint.data-uri";
  String MAX_CONCURRENT_CHECKPOINTS = "execution.max-concurrent-checkpoints";
  String CHECKPOINT_CLEANUP_MODE = "execution.checkpoint.cleanup-mode";
  String MIN_PAUSE_BETWEEN_CHECKPOINTS = "execution.checkpoint.min-pause";
  String FAIL_ON_CHECKPOINTING_ERRORS = "execution.checkpoint.fail-on-error";
  String RESTART_STRATEGY = "execution.restart.strategy";
  String RESTART_ATTEMPTS = "execution.restart.attempts";
  String RESTART_DELAY_BETWEEN_ATTEMPTS = "execution.restart.delayBetweenAttempts";
  String RESTART_FAILURE_INTERVAL = "execution.restart.failureInterval";
  String RESTART_FAILURE_RATE = "execution.restart.failureRate";
  String RESTART_DELAY_INTERVAL = "execution.restart.delayInterval";
  String MAX_STATE_RETENTION_TIME = "execution.query.state.max-retention";
  String MIN_STATE_RETENTION_TIME = "execution.query.state.min-retention";
  String STATE_BACKEND = "execution.state.backend";
  String PLANNER = "execution.planner";
}
