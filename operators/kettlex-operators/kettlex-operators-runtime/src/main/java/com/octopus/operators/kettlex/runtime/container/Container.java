package com.octopus.operators.kettlex.runtime.container;

import com.octopus.operators.kettlex.runtime.monitor.ContainerCommunicator;

public interface Container {

  void init();

  void start();

  void stop();

  ContainerCommunicator getContainerCommunicator();
}
