package com.octopus.kettlex.runtime.container;

public interface Container {

  String getContainerId();

  String getContainerName();

  void init();

  void start();

  void stop();

  void destroy();
}
