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
public class CronJobSpec {

  private String cron;
  private Integer startingDeadlineSeconds;
  private CronJobConcurrencyPolicy concurrencyPolicy;
  private KubernetesMetaData metadata;
  private Map<String, String> nodeSelector;
  private PodTemplate template;

}
