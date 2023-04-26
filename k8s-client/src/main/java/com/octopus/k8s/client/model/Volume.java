package com.octopus.k8s.client.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Volume {

  private String name;
  private KeyPathItem configMapItem;
  private KeyPathItem secretItem;

}
