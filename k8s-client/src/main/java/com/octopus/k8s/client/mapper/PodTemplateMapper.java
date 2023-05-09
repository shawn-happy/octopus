package com.octopus.k8s.client.mapper;

import com.octopus.k8s.client.model.Container;
import com.octopus.k8s.client.model.ContainerEnv;
import com.octopus.k8s.client.model.ContainerEnvValueSource;
import com.octopus.k8s.client.model.ContainerPort;
import com.octopus.k8s.client.model.HttpRequest;
import com.octopus.k8s.client.model.PodTemplateSpec;
import com.octopus.k8s.client.model.PodTemplate;
import com.octopus.k8s.client.model.Probe;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.models.V1ConfigMapKeySelector;
import io.kubernetes.client.openapi.models.V1ConfigMapKeySelectorBuilder;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1ContainerPortBuilder;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1EnvVarSourceBuilder;
import io.kubernetes.client.openapi.models.V1ExecAction;
import io.kubernetes.client.openapi.models.V1ExecActionBuilder;
import io.kubernetes.client.openapi.models.V1HTTPGetAction;
import io.kubernetes.client.openapi.models.V1HTTPGetActionBuilder;
import io.kubernetes.client.openapi.models.V1HTTPHeader;
import io.kubernetes.client.openapi.models.V1HTTPHeaderBuilder;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelectorBuilder;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpecBuilder;
import io.kubernetes.client.openapi.models.V1Probe;
import io.kubernetes.client.openapi.models.V1ProbeBuilder;
import io.kubernetes.client.openapi.models.V1SecretKeySelector;
import io.kubernetes.client.openapi.models.V1SecretKeySelectorBuilder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class PodTemplateMapper {

  public static V1PodTemplateSpec map(@Nonnull PodTemplate template) {
    return new V1PodTemplateSpecBuilder()
        .withMetadata(KubernetesMetaDataMapper.map(template.getMetaData()))
        .withSpec(map(template.getSpec()))
        .build();
  }

  private static V1PodSpec map(@Nonnull PodTemplateSpec spec) {
    return new V1PodSpecBuilder()
        .withContainers(toV1Container(spec.getContainers()))
        .withNodeName(spec.getNodeName())
        .withNodeSelector(spec.getNodeSelector())
        .withRestartPolicy(spec.getRestartPolicy().name())
        .withServiceAccount(spec.getServiceAccount())
        .withServiceAccountName(spec.getServiceAccountName())
        .withInitContainers(toV1Container(spec.getInitContainers()))
        .withVolumes(VolumeMapper.toVolumes(spec.getVolumes()))
        .build();
  }

  private static List<V1Container> toV1Container(List<Container> containerList) {
    if (CollectionUtils.isEmpty(containerList)) {
      return null;
    }
    return containerList.stream().map(PodTemplateMapper::map).collect(Collectors.toList());
  }

  private static V1Container map(Container container) {
    return new V1ContainerBuilder()
        .withName(container.getName())
        .withImage(container.getImage())
        .withImagePullPolicy(container.getImagePullPolicy().name())
        .withEnv(map(container.getEnvs()))
        .withVolumeMounts(VolumeMapper.toVolumeMounts(container.getVolumeMounts()))
        .withLivenessProbe(toProbe(container.getLivenessProbe()))
        .withReadinessProbe(toProbe(container.getReadinessProbe()))
        .withPorts(toContainerPorts(container.getPorts()))
        .withArgs(container.getArgs())
        .withCommand(container.getCommand())
        .build();
  }

  private static List<V1EnvVar> map(List<ContainerEnv> containerEnvs) {
    if (CollectionUtils.isEmpty(containerEnvs)) {
      return null;
    }
    return containerEnvs.stream().map(PodTemplateMapper::map).collect(Collectors.toList());
  }

  private static V1EnvVar map(ContainerEnv env) {
    if (env == null) {
      return null;
    }
    return new V1EnvVarBuilder()
        .withName(env.getName())
        .withValue(env.getValue())
        .withValueFrom(map(env.getValueFrom()))
        .build();
  }

  private static V1EnvVarSource map(ContainerEnvValueSource source) {
    if (source == null) {
      return null;
    }
    return new V1EnvVarSourceBuilder()
        .withConfigMapKeyRef(
            toConfigMapKeySelector(source.getConfigMapName(), source.getConfigMapKey()))
        .withSecretKeyRef(toSecretKeySelector(source.getSecretName(), source.getSecretKey()))
        .withFieldRef(toFieldSelector(source.getFieldPath()))
        .build();
  }

  private static V1ConfigMapKeySelector toConfigMapKeySelector(String name, String key) {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    return new V1ConfigMapKeySelectorBuilder().withKey(key).withName(name).build();
  }

  private static V1SecretKeySelector toSecretKeySelector(String name, String key) {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    return new V1SecretKeySelectorBuilder().withKey(key).withName(name).build();
  }

  private static V1ObjectFieldSelector toFieldSelector(String fieldPath) {
    if (StringUtils.isBlank(fieldPath)) {
      return null;
    }
    return new V1ObjectFieldSelectorBuilder().withFieldPath(fieldPath).build();
  }

  private static V1Probe toProbe(Probe probe) {
    if (probe == null) {
      return null;
    }
    return new V1ProbeBuilder()
        .withTimeoutSeconds(probe.getTimeoutSeconds())
        .withSuccessThreshold(probe.getSuccessThreshold())
        .withPeriodSeconds(probe.getPeriodSeconds())
        .withFailureThreshold(probe.getFailureThreshold())
        .withInitialDelaySeconds(probe.getInitialDelaySeconds())
        .withHttpGet(toHttpGetAction(probe.getHttpRequest()))
        .withExec(toExecAction(probe.getCommand()))
        .build();
  }

  private static V1ExecAction toExecAction(String... command) {
    return new V1ExecActionBuilder().withCommand(command).build();
  }

  private static V1HTTPGetAction toHttpGetAction(HttpRequest httpRequest) {
    if (httpRequest == null) {
      return null;
    }
    Map<String, String> headers = httpRequest.getHeaders();
    V1HTTPHeader[] v1HTTPHeaders = null;
    if (MapUtils.isNotEmpty(headers)) {
      v1HTTPHeaders = new V1HTTPHeader[headers.size()];
      int i = 0;
      for (String key : headers.keySet()) {
        String value = headers.get(key);
        V1HTTPHeader httpHeader = new V1HTTPHeaderBuilder().withName(key).withValue(value).build();
        v1HTTPHeaders[i] = httpHeader;
        i++;
      }
    }
    return new V1HTTPGetActionBuilder()
        .withHost(httpRequest.getHost())
        .withPath(httpRequest.getPath())
        .withPort(new IntOrString(httpRequest.getPort()))
        .withHttpHeaders(v1HTTPHeaders)
        .build();
  }

  private static List<V1ContainerPort> toContainerPorts(List<ContainerPort> ports) {
    if (CollectionUtils.isEmpty(ports)) {
      return null;
    }
    return ports.stream().map(PodTemplateMapper::toContainerPort).collect(Collectors.toList());
  }

  private static V1ContainerPort toContainerPort(ContainerPort port) {
    if (port == null) {
      return null;
    }
    return new V1ContainerPortBuilder()
        .withName(port.getName())
        .withContainerPort(port.getPort())
        .withProtocol(port.getProtocol())
        .build();
  }
}
