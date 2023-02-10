package com.shawn.octopus.k8s.client.spi;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;

public class KubernetesApiClient {

  public final ApiClient apiClient;
  private final CoreV1Api coreV1Api;

  public KubernetesApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
    this.coreV1Api = new CoreV1Api(apiClient);
  }

  public void createPod() throws ApiException {
    V1Pod v1Pod = new V1Pod();
    CoreV1Api coreV1Api = new CoreV1Api(apiClient);
    coreV1Api.createNamespacedPod("", v1Pod, "true", null, null);
  }
}
