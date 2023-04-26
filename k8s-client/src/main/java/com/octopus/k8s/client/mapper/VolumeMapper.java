package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.KeyPath;
import com.octopus.k8s.client.model.KeyPathItem;
import com.octopus.k8s.client.model.Volume;
import com.octopus.k8s.client.model.VolumeMount;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1EmptyDirVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1KeyToPath;
import io.kubernetes.client.openapi.models.V1KeyToPathBuilder;
import io.kubernetes.client.openapi.models.V1SecretVolumeSource;
import io.kubernetes.client.openapi.models.V1SecretVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class VolumeMapper {

  public static List<V1VolumeMount> toVolumeMounts(List<VolumeMount> volumeMounts) {
    if (CollectionUtils.isEmpty(volumeMounts)) {
      return null;
    }
    return volumeMounts.stream().map(VolumeMapper::toVolumeMount).collect(Collectors.toList());
  }

  public static V1VolumeMount toVolumeMount(VolumeMount volumeMount) {
    if (volumeMount == null) {
      return null;
    }
    return new V1VolumeMountBuilder()
        .withName(volumeMount.getName())
        .withMountPath(volumeMount.getMountPath())
        .withSubPath(volumeMount.getSubPath())
        .build();
  }

  public static List<V1Volume> toVolumes(List<Volume> volumes) {
    if (CollectionUtils.isEmpty(volumes)) {
      return null;
    }
    return volumes.stream().map(VolumeMapper::toVolume).collect(Collectors.toList());
  }

  public static V1Volume toVolume(Volume volume) {
    if (volume == null) {
      return null;
    }
    return new V1VolumeBuilder()
        .withName(volume.getName())
        .withConfigMap(toConfigMapVolumeSource(volume.getConfigMapItem()))
        .withSecret(toSecretVolumeSource(volume.getSecretItem()))
        .build();
  }

  private static V1ConfigMapVolumeSource toConfigMapVolumeSource(KeyPathItem item) {
    if (item == null) {
      return null;
    }
    return new V1ConfigMapVolumeSourceBuilder()
        .withName(item.getName())
        .withItems(toKeyPaths(item.getItems()))
        .build();
  }

  private static V1SecretVolumeSource toSecretVolumeSource(KeyPathItem item) {
    if (item == null) {
      return null;
    }
    return new V1SecretVolumeSourceBuilder()
        .withSecretName(item.getName())
        .withItems(toKeyPaths(item.getItems()))
        .build();
  }

  private static List<V1KeyToPath> toKeyPaths(List<KeyPath> keyPaths) {
    if (CollectionUtils.isEmpty(keyPaths)) {
      return null;
    }
    return keyPaths.stream().map(VolumeMapper::toKeyPath).collect(Collectors.toList());
  }

  private static V1KeyToPath toKeyPath(KeyPath keyPath) {
    if (keyPath == null) {
      return null;
    }
    return new V1KeyToPathBuilder().withKey(keyPath.getKey()).withPath(keyPath.getPath()).build();
  }
}
