package com.octopus.k8s.client.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ContainerResource {

  private String requestCPU;
  private String requestMemory;
  private String limitCPU;
  private String limitMemory;
}
