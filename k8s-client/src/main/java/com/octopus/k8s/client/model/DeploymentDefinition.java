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
public class DeploymentDefinition implements KubernetesResourceDefinition {

  private String apiVersion;
  private KubernetesResourceKind kind;
  private KubernetesMetaData metadata;
  private DeploymentSpec spec;
}
