package com.octopus.k8s.client.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeploymentSpec {

  private LabelSelector selector;
  private Integer replica;
  private CreateResourceStrategy strategy;
  private PodTemplate template;

}
