package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.KubernetesResourceDefinition;

public interface KubernetesResourceMapper<S extends KubernetesResourceDefinition, T> {

  T map(S s);

}
