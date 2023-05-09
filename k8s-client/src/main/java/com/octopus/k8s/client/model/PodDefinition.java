package com.octopus.k8s.client.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PodDefinition implements KubernetesResourceDefinition {

  private String apiVersion;
  private KubernetesResourceKind kind;
  private KubernetesMetaData metadata;
  private PodSpec spec;
}
