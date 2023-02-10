package com.shawn.octopus.k8s.client.spi;

public interface KubernetesResource {

  String getApiVersion();

  SupportedKubernetesResource getKind();

  KubernetesMetaData getMetadata();
}
