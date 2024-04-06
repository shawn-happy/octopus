package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

public class HttpUtil {
  private final HttpClientBuilder httpClientBuilder =
      HttpClients.custom()
          .setRedirectStrategy(
              new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                  return true;
                }
              });

  public CloseableHttpClient getHttpClient() {
    return httpClientBuilder.build();
  }
}
