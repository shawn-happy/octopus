package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.PodDefinition;
import com.octopus.k8s.client.model.PodSpec;
import com.octopus.k8s.client.model.PodTemplate;
import io.kubernetes.client.openapi.models.V1DeploymentBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import javax.annotation.Nonnull;

public class DefaultPodMapper implements PodMapper<V1Pod> {

  @Override
  public V1Pod map(PodDefinition podDefinition) {
    if (podDefinition == null) {
      return null;
    }
    return new V1PodBuilder()
        .withApiVersion(podDefinition.getApiVersion())
        .withKind(podDefinition.getKind().name())
        .withSpec(PodTemplateMapper.map(podDefinition.getSpec().getTemplate()).getSpec())
        .withMetadata(KubernetesMetaDataMapper.map(podDefinition.getMetadata()))
        .build();
  }
}
