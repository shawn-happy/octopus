package com.shawn.octopus.k8s.client.spi;

import java.util.Map;

public class KubernetesMetaData {

  private String namespace;
  private String name;
  private String uid;
  private String clusterName;
  private Long creationTimestamp;
  private Map<String, String> labels;
  private Map<String, String> annotations;

  public KubernetesMetaData() {}

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public Long getCreationTimestamp() {
    return creationTimestamp;
  }

  public void setCreationTimestamp(Long creationTimestamp) {
    this.creationTimestamp = creationTimestamp;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  public void setAnnotations(Map<String, String> annotations) {
    this.annotations = annotations;
  }

  public static KubernetesMetaDataBuilder builder() {
    return new KubernetesMetaDataBuilder();
  }

  public static class KubernetesMetaDataBuilder {

    private String namespace;
    private String name;
    private String uid;
    private String clusterName;
    private Long creationTimestamp;
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

    public KubernetesMetaDataBuilder uid(String uid) {
      this.uid = uid;
      return this;
    }

    public KubernetesMetaDataBuilder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public KubernetesMetaDataBuilder creationTimestamp(Long creationTimestamp) {
      this.creationTimestamp = creationTimestamp;
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
      kubernetesMetaData.setNamespace(namespace);
      kubernetesMetaData.setName(name);
      kubernetesMetaData.setClusterName(clusterName);
      kubernetesMetaData.setUid(uid);
      kubernetesMetaData.setLabels(labels);
      kubernetesMetaData.setAnnotations(annotations);
      kubernetesMetaData.setCreationTimestamp(creationTimestamp);
      return kubernetesMetaData;
    }
  }
}
