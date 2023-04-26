package com.octopus.k8s.client.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Probe {

  private String[] command;
  private Integer failureThreshold;
  private Integer initialDelaySeconds;
  private Integer periodSeconds;
  private Integer successThreshold;
  private Integer timeoutSeconds;
  private HttpRequest httpRequest;
}
