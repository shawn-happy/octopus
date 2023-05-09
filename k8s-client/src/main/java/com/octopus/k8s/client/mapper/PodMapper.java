package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.PodDefinition;

public interface PodMapper<T> extends KubernetesResourceMapper<PodDefinition, T>{

}
