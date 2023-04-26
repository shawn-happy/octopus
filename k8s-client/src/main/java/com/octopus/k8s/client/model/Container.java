package com.octopus.k8s.client.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Container {

  private String name;
  private String image;
  private ImagePullPolicy imagePullPolicy;
  private String[] command;
  private String[] args;
  private ContainerResource resource;
  private List<ContainerEnv> envs;
  private List<VolumeMount> volumeMounts;
  private Probe readinessProbe;
  private Probe livenessProbe;
  private List<ContainerPort> ports;
}
