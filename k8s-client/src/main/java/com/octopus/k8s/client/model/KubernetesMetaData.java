package com.octopus.k8s.client.model;

import java.util.Map;

public class KubernetesMetaData {

  private String namespace;
  private String name;
  private Map<String, String> labels;
  private Map<String, String> annotations;

  public KubernetesMetaData() {}

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  public static KubernetesMetaDataBuilder builder() {
    return new KubernetesMetaDataBuilder();
  }

  public static class KubernetesMetaDataBuilder {

    private String namespace;
    private String name;
    private Map<String, String> labels;
    private Map<String, String> annotations;

    public KubernetesMetaDataBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public KubernetesMetaDataBuilder name(String name) {
      this.name = name;
      return this;
    }

    public KubernetesMetaDataBuilder labels(Map<String, String> labels) {
      this.labels = labels;
      return this;
    }

    public KubernetesMetaDataBuilder annotations(Map<String, String> annotations) {
      this.annotations = annotations;
      return this;
    }

    public KubernetesMetaData build() {
      KubernetesMetaData kubernetesMetaData = new KubernetesMetaData();
      kubernetesMetaData.namespace = this.namespace;
      kubernetesMetaData.name = this.name;
      kubernetesMetaData.labels = this.labels;
      kubernetesMetaData.annotations = this.annotations;
      return kubernetesMetaData;
    }
  }
}
