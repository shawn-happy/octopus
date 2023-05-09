package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.CreateResourceStrategy;
import com.octopus.k8s.client.model.DeploymentDefinition;
import com.octopus.k8s.client.model.DeploymentSpec;
import com.octopus.k8s.client.model.LabelSelector;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentBuilder;
import io.kubernetes.client.openapi.models.V1DeploymentSpec;
import io.kubernetes.client.openapi.models.V1DeploymentSpecBuilder;
import io.kubernetes.client.openapi.models.V1DeploymentStrategy;
import io.kubernetes.client.openapi.models.V1DeploymentStrategyBuilder;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1LabelSelectorBuilder;
import javax.annotation.Nonnull;

public class DefaultDeploymentMapper implements DeploymentMapper<V1Deployment> {

  @Override
  public V1Deployment map(DeploymentDefinition deploymentDefinition) {
    if (deploymentDefinition == null) {
      return null;
    }
    return new V1DeploymentBuilder()
        .withApiVersion(deploymentDefinition.getApiVersion())
        .withKind(deploymentDefinition.getKind().name())
        .withMetadata(KubernetesMetaDataMapper.map(deploymentDefinition.getMetadata()))
        .withSpec(map(deploymentDefinition.getSpec()))
        .build();
  }

  private V1DeploymentSpec map(@Nonnull DeploymentSpec spec) {
    return new V1DeploymentSpecBuilder()
        .withSelector(map(spec.getSelector()))
        .withReplicas(spec.getReplica())
        .withStrategy(map(spec.getStrategy()))
        .withTemplate(PodTemplateMapper.map(spec.getTemplate()))
        .build();
  }

  private V1LabelSelector map(LabelSelector selector) {
    if (selector == null) {
      return null;
    }
    return new V1LabelSelectorBuilder().withMatchLabels(selector.getLabels()).build();
  }

  private V1DeploymentStrategy map(CreateResourceStrategy strategy) {
    if (strategy == null) {
      return null;
    }
    return new V1DeploymentStrategyBuilder().withType(strategy.name()).build();
  }
}
