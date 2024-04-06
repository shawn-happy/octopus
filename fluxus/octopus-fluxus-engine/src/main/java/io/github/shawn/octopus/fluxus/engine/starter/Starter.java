package io.github.shawn.octopus.fluxus.engine.starter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Starter {

  public static void main(String[] args) {
    JobExecutor jobExecutor = new JobExecutor(args[0]);
    jobExecutor.execute();
  }
}
