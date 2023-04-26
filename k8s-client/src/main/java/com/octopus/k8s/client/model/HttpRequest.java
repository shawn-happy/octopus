package com.octopus.k8s.client.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class HttpRequest {

  private String host;
  private int port;
  private String path;
  private Map<String, String> headers;
}
