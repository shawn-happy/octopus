package com.octopus.k8s.client.model;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PodTemplateSpec {

  private List<Container> containers;
  private List<Container> initContainers;
  private Map<String, String> nodeSelector;
  private String nodeName;
  private String serviceAccount;
  private String serviceAccountName;
  @Default private PodRestartPolicy restartPolicy = PodRestartPolicy.Always;
  private List<Volume> volumes;

}
