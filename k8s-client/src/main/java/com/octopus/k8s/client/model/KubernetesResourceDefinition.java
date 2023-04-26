package com.octopus.k8s.client.model;

public interface KubernetesResourceDefinition {

  String getApiVersion();

  KubernetesResourceKind getKind();

  KubernetesMetaData getMetadata();
}
