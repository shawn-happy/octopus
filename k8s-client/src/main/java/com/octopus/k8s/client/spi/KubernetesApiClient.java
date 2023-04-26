package com.octopus.k8s.client.spi;

import com.octopus.k8s.client.mapper.DefaultDeploymentMapper;
import com.octopus.k8s.client.model.DeploymentDefinition;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.extended.wait.Wait;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMapKeySelector;
import io.kubernetes.client.openapi.models.V1ConfigMapKeySelectorBuilder;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1ContainerPortBuilder;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentBuilder;
import io.kubernetes.client.openapi.models.V1DeploymentSpecBuilder;
import io.kubernetes.client.openapi.models.V1DeploymentStrategyBuilder;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1EnvVarSourceBuilder;
import io.kubernetes.client.openapi.models.V1ExecActionBuilder;
import io.kubernetes.client.openapi.models.V1HTTPGetActionBuilder;
import io.kubernetes.client.openapi.models.V1HTTPHeader;
import io.kubernetes.client.openapi.models.V1HTTPHeaderBuilder;
import io.kubernetes.client.openapi.models.V1LabelSelectorBuilder;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelectorBuilder;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1PodTemplateSpecBuilder;
import io.kubernetes.client.openapi.models.V1ProbeBuilder;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1ResourceRequirementsBuilder;
import io.kubernetes.client.openapi.models.V1SecretKeySelectorBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;
import io.kubernetes.client.proto.V1.HTTPGetAction;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubernetesApiClient {

  private final ApiClient apiClient;
  private final AppsV1Api appsV1Api;
  private final BatchV1Api batchV1Api;
  private final CoreV1Api coreV1Api;
  private final ExtensionsV1beta1Api extensionsV1beta1Api;
  private final CustomObjectsApi customObjectsApi;
  private final RbacAuthorizationV1Api rbacAuthorizationV1Api;

  public KubernetesApiClient() throws IOException {
    this(Config.defaultClient());
  }

  public KubernetesApiClient(String configPath) throws IOException {
    this(Config.fromConfig(configPath));
  }

  public KubernetesApiClient(ApiClient apiClient) throws IOException {
    this.apiClient = apiClient;
    Configuration.setDefaultApiClient(apiClient);
    this.appsV1Api = new AppsV1Api(apiClient);
    this.batchV1Api = new BatchV1Api(apiClient);
    this.coreV1Api = new CoreV1Api(apiClient);
    this.extensionsV1beta1Api = new ExtensionsV1beta1Api(apiClient);
    this.customObjectsApi = new CustomObjectsApi(apiClient);
    this.rbacAuthorizationV1Api = new RbacAuthorizationV1Api(apiClient);
  }

  public void createDeployment(DeploymentDefinition definition) throws ApiException {
    V1Deployment v1Deployment =
        appsV1Api.readNamespacedDeployment(
            definition.getMetadata().getName(),
            definition.getMetadata().getNamespace(),
            "true",
            null,
            null);
    if (v1Deployment != null) {
      return;
    }
    V1Deployment deployment = new DefaultDeploymentMapper().map(definition);
    appsV1Api.createNamespacedDeployment(
        definition.getMetadata().getNamespace(), deployment, "true", null, null);

    Wait.poll(
        Duration.ofSeconds(3),
        Duration.ofSeconds(60),
        () -> {
          try {
            log.info("Waiting until example deployment is ready...");
            return Optional.ofNullable(
                        Objects.requireNonNull(
                                Optional.ofNullable(
                                        appsV1Api.readNamespacedDeployment(
                                            definition.getMetadata().getName(),
                                            definition.getMetadata().getNamespace(),
                                            "true",
                                            null,
                                            null))
                                    .orElseThrow(RuntimeException::new)
                                    .getStatus())
                            .getReadyReplicas())
                    .orElse(-1)
                > 0;
          } catch (ApiException e) {
            e.printStackTrace();
            return false;
          }
        });
  }

  public void createPod() throws ApiException {
    V1Pod v1Pod = new V1Pod();
    coreV1Api.createNamespacedPod("", v1Pod, "true", null, null);
  }

  private List<V1HTTPHeader> build(Map<String, String> headers) {
    List<V1HTTPHeader> headerList = new ArrayList<>(headers.size());
    for (String key : headers.keySet()) {
      headerList.add(new V1HTTPHeaderBuilder().withName(key).withValue(headers.get(key)).build());
    }
    return headerList;
  }
}
