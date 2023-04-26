package com.octopus.k8s.client.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ContainerPort {

  private Integer port;
  private String name;
  private String protocol;
}
