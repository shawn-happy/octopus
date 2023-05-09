package com.octopus.k8s.client.spi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.octopus.k8s.client.mapper.CronJobMapper;
import com.octopus.k8s.client.mapper.DefaultDeploymentMapper;
import com.octopus.k8s.client.mapper.DefaultPodMapper;
import com.octopus.k8s.client.model.CronJobDefinition;
import com.octopus.k8s.client.model.DeploymentDefinition;
import com.octopus.k8s.client.model.KubernetesResourceKind;
import com.octopus.k8s.client.model.PodDefinition;
import io.kubernetes.client.extended.wait.Wait;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.BatchV1beta1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1beta1CronJob;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class KubernetesApiClient {

  private final ApiClient apiClient;
  private final AppsV1Api appsV1Api;
  private final BatchV1Api batchV1Api;
  private final BatchV1beta1Api batchV1beta1Api;
  private final CoreV1Api coreV1Api;
  private final ExtensionsV1beta1Api extensionsV1beta1Api;
  private final CustomObjectsApi customObjectsApi;
  private final RbacAuthorizationV1Api rbacAuthorizationV1Api;

  private static final Gson GSON = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

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
    this.batchV1beta1Api = new BatchV1beta1Api(apiClient);
    this.coreV1Api = new CoreV1Api(apiClient);
    this.extensionsV1beta1Api = new ExtensionsV1beta1Api(apiClient);
    this.customObjectsApi = new CustomObjectsApi(apiClient);
    this.rbacAuthorizationV1Api = new RbacAuthorizationV1Api(apiClient);
  }

  public void createIfNotConflict(@Nonnull Map<String, Object> config) throws ApiException {
    Map<String, Object> metadata = (Map<String, Object>) config.get("metadata");
    var name = metadata.get("name").toString();
    var namespace = metadata.get("namespace").toString();
    var kind = KubernetesResourceKind.valueOf(config.get("kind").toString());
    try {
      switch (kind) {
        case ConfigMap:
          V1ConfigMap configMap = convertOrThrow(config, V1ConfigMap.class);
          coreV1Api.createNamespacedConfigMap(namespace, configMap, "true", null, null);
          break;
        case CronJob:
          V1beta1CronJob cronJob = convertOrThrow(config, V1beta1CronJob.class);
          batchV1beta1Api.createNamespacedCronJob(namespace, cronJob, "true", null, null);
          break;
        case Service:
          V1Service service = convertOrThrow(config, V1Service.class);
          coreV1Api.createNamespacedService(namespace, service, "true", null, null);
          break;
        case ServiceAccount:
          V1ServiceAccount v1ServiceAccount = convertOrThrow(config, V1ServiceAccount.class);
          coreV1Api.createNamespacedServiceAccount(namespace, v1ServiceAccount, "true", null, null);
          break;
        case Role:
          var v1Role = convertOrThrow(config, V1Role.class);
          rbacAuthorizationV1Api.createNamespacedRole(namespace, v1Role, "true", null, null);
          break;
        case RoleBinding:
          var v1RoleBinding = convertOrThrow(config, V1RoleBinding.class);
          rbacAuthorizationV1Api.createNamespacedRoleBinding(
              namespace, v1RoleBinding, "true", null, null);
          break;
        case Pod:
          var pod = convertOrThrow(config, V1Pod.class);
          coreV1Api.createNamespacedPod(namespace, pod, "true", null, null);
          break;
        case Deployment:
          var deployment = convertOrThrow(config, V1Deployment.class);
          appsV1Api.createNamespacedDeployment(namespace, deployment, "true", null, null);
          break;
        case StatefulSet:
          var v1StatefulSet = convertOrThrow(config, V1StatefulSet.class);
          appsV1Api.createNamespacedStatefulSet(namespace, v1StatefulSet, "true", null, null);
          break;
        default:
          String apiVersionStr = (String) config.get("apiVersion");
          String[] tokens = apiVersionStr.split("/");
          customObjectsApi.createNamespacedCustomObject(
              tokens[0],
              tokens[1],
              namespace,
              config.get("kind").toString().toLowerCase() + "s",
              config,
              "true",
              null,
              null);
          break;
      }
    } catch (ApiException ex) {
      if (ex.getCode() == 409) {
        log.info("Got 409, Resource already exist, skip. [namespace={}, name={}]", namespace, name);
        return; // already exist
      }
      log.error("kube api get error response: " + ex.getResponseBody());
      throw ex;
    }
  }

  public void createDeployment(DeploymentDefinition definition) {
    try {
      var v1Deployment =
          appsV1Api.readNamespacedDeployment(
              definition.getMetadata().getName(),
              definition.getMetadata().getNamespace(),
              "true",
              null,
              null);
      if (v1Deployment != null) {
        return;
      }
      var deployment = new DefaultDeploymentMapper().map(definition);
      appsV1Api.createNamespacedDeployment(
          definition.getMetadata().getNamespace(), deployment, "true", null, null);
    } catch (ApiException ex) {
      if (ex.getCode() == 409) {
        log.info(
            "Got 409, Resource already exist, skip. [namespace={}, name={}]",
            definition.getMetadata().getNamespace(),
            definition.getMetadata().getName());
        return; // already exist
      }
      log.error("kube api get error response: " + ex.getResponseBody());
      throw new RuntimeException(ex);
    }
  }

  public void createPod(PodDefinition definition) {
    try {
      var v1pod =
          coreV1Api.readNamespacedPod(
              definition.getMetadata().getName(),
              definition.getMetadata().getNamespace(),
              "true",
              null,
              null);
      if (v1pod != null) {
        return;
      }
      var pod = new DefaultPodMapper().map(definition);
      coreV1Api.createNamespacedPod(
          definition.getMetadata().getNamespace(), pod, "true", null, null);
    } catch (ApiException ex) {
      if (ex.getCode() == 409) {
        log.info(
            "Got 409, Resource already exist, skip. [namespace={}, name={}]",
            definition.getMetadata().getNamespace(),
            definition.getMetadata().getName());
        return; // already exist
      }
      log.error("kube api get error response: " + ex.getResponseBody());
      throw new RuntimeException(ex);
    }
  }

  public void createCronJob(CronJobDefinition definition) {
    try {
      var cronJob =
          batchV1beta1Api.readNamespacedCronJob(
              definition.getMetadata().getName(),
              definition.getMetadata().getNamespace(),
              "true",
              null,
              null);
      if (cronJob != null) {
        return;
      }

      var cj = CronJobMapper.map(definition);
      batchV1beta1Api.createNamespacedCronJob(
          definition.getMetadata().getNamespace(), cj, "true", null, null);
    } catch (ApiException ex) {
      if (ex.getCode() == 409) {
        log.info(
            "Got 409, Resource already exist, skip. [namespace={}, name={}]",
            definition.getMetadata().getNamespace(),
            definition.getMetadata().getName());
        return; // already exist
      }
      log.error("kube api get error response: " + ex.getResponseBody());
      throw new RuntimeException(ex);
    }
  }

  private <T> T convertOrThrow(Map<String, Object> fromValue, Class<T> type) {
    // 在解析 requests->(cpu, memory),
    // limits->(cpu, memory)的时候，不能解析成io.kubernetes.client.custom.Quantity
    // io.kubernetes.client.custom.Quantity上的json 注解都是gson的，所以改成了gson的实现
    try {
      String jsonString = GSON.toJson(fromValue);
      if (StringUtils.isNotBlank(jsonString)) {
        return GSON.fromJson(jsonString, type);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  private void executeThrowException(Runnable action, Supplier<Throwable> throwableSupplier) {
    try {
      action.run();
    } catch (Throwable t) {
      log.error("{}", throwableSupplier.get().getMessage(), throwableSupplier.get());
      throw new RuntimeException(
          throwableSupplier.get().getMessage(), throwableSupplier.get().initCause(t));
    }
  }
}
