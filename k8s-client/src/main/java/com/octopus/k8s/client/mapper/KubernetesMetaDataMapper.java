package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.KubernetesMetaData;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import javax.annotation.Nonnull;

public class KubernetesMetaDataMapper {

  public static V1ObjectMeta map(@Nonnull KubernetesMetaData metaData) {
    return new V1ObjectMetaBuilder()
        .withName(metaData.getName())
        .withNamespace(metaData.getNamespace())
        .withLabels(metaData.getLabels())
        .withAnnotations(metaData.getAnnotations())
        .build();
  }
}
