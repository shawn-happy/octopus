package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.DeploymentDefinition;

public interface DeploymentMapper<T> extends KubernetesResourceMapper<DeploymentDefinition, T>{

}
