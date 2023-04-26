package com.octopus.k8s.client.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ContainerEnvValueSource {

  private String configMapKey;
  private String configMapName;
  private String fieldPath;
  private String secretKey;
  private String secretName;

}
