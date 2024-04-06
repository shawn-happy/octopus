package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.annotations.VisibleForTesting;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.DorisSinkConfig;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.exception.DorisConnectorException;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model.Backend;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model.BackendRow;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model.BackendV2;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;

@Slf4j
public class RestService {
  @Deprecated private static final String BACKENDS = "/rest/v1/system?path=//backends";
  private static final String BACKENDS_V2 = "/api/backends?is_alive=true";
  private static final String BASE_URL = "http://%s%s";

  private static final String ILLEGAL_ARGUMENT_MESSAGE = "argument '%s' is illegal, value is '%s'.";
  private static final String SHOULD_NOT_HAPPEN_MESSAGE = "Should not come here.";

  private static String send(DorisSinkConfig.DorisSinkOptions dorisConfig, HttpRequestBase request)
      throws DorisConnectorException {
    int connectTimeout = dorisConfig.getRequestConnectTimeoutMs();
    int socketTimeout = dorisConfig.getRequestReadTimeoutMs();
    int retries = dorisConfig.getRequestRetries();
    log.info(
        "connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
        connectTimeout,
        socketTimeout,
        retries);

    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(connectTimeout)
            .setSocketTimeout(socketTimeout)
            .build();

    request.setConfig(requestConfig);
    log.info(
        "Send request to Doris FE '{}' with user '{}'.",
        request.getURI(),
        dorisConfig.getUsername());
    IOException ex = null;
    int statusCode = -1;

    for (int attempt = 0; attempt < retries; attempt++) {
      log.debug("Attempt {} to request {}.", attempt, request.getURI());
      try {
        String response;
        if (request instanceof HttpGet) {
          response =
              getConnectionGet(
                  request.getURI().toString(),
                  dorisConfig.getUsername(),
                  dorisConfig.getPassword());
        } else {
          response =
              getConnectionPost(request, dorisConfig.getUsername(), dorisConfig.getPassword());
        }
        log.info(
            "Success get response from Doris FE: {}, response is: {}.", request.getURI(), response);
        // Handle the problem of inconsistent data format returned by http v1 and v2
        Map<String, Object> map =
            JsonUtils.fromJson(response, new TypeReference<Map<String, Object>>() {}).get();
        if (map.containsKey("code") && map.containsKey("msg")) {
          Object data = map.get("data");
          return JsonUtils.toJson(data).get();
        } else {
          return response;
        }
      } catch (IOException e) {
        ex = e;
        log.warn("{}", request.getURI(), e);
      }
    }
    String errMsg =
        "Connect to " + request.getURI().toString() + "failed, status code is " + statusCode + ".";
    throw new DorisConnectorException(errMsg, ex);
  }

  private static String getConnectionPost(HttpRequestBase request, String user, String passwd)
      throws IOException {
    URL url = new URL(request.getURI().toString());
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setInstanceFollowRedirects(false);
    conn.setRequestMethod(request.getMethod());
    String authEncoding =
        Base64.getEncoder()
            .encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
    conn.setRequestProperty("Authorization", "Basic " + authEncoding);
    InputStream content = ((HttpPost) request).getEntity().getContent();
    String res = IOUtils.toString(content, StandardCharsets.UTF_8);
    conn.setDoOutput(true);
    conn.setDoInput(true);
    PrintWriter out = new PrintWriter(conn.getOutputStream());
    // send request params
    out.print(res);
    // flush
    out.flush();
    // read response
    return parseResponse(conn);
  }

  private static String getConnectionGet(String request, String user, String passwd)
      throws IOException {
    URL realUrl = new URL(request);
    // open connection
    HttpURLConnection connection = (HttpURLConnection) realUrl.openConnection();
    String authEncoding =
        Base64.getEncoder()
            .encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + authEncoding);

    connection.connect();
    return parseResponse(connection);
  }

  private static String parseResponse(HttpURLConnection connection) throws IOException {
    if (connection.getResponseCode() != HttpStatus.SC_OK) {
      log.warn(
          "Failed to get response from Doris  {}, http code is {}",
          connection.getURL(),
          connection.getResponseCode());
      throw new IOException("Failed to get response from Doris");
    }
    StringBuilder result = new StringBuilder();
    try (BufferedReader in =
        new BufferedReader(
            new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = in.readLine()) != null) {
        result.append(line);
      }
    } catch (IOException e) {
      throw new IOException(e);
    }
    return result.toString();
  }

  @VisibleForTesting
  static String randomEndpoint(String feNodes) throws DorisConnectorException {
    log.trace("Parse feNodes '{}'.", feNodes);
    if (StringUtils.isEmpty(feNodes)) {
      String errMsg = String.format(ILLEGAL_ARGUMENT_MESSAGE, "feNodes", feNodes);
      throw new DorisConnectorException(errMsg);
    }
    List<String> nodes = Arrays.asList(feNodes.split(","));
    Collections.shuffle(nodes);
    return nodes.get(0).trim();
  }

  @VisibleForTesting
  static List<String> allEndpoints(String feNodes) throws DorisConnectorException {
    log.trace("Parse feNodes '{}'.", feNodes);
    if (StringUtils.isEmpty(feNodes)) {
      String errMsg = String.format(ILLEGAL_ARGUMENT_MESSAGE, "feNodes", feNodes);
      throw new DorisConnectorException(errMsg);
    }
    List<String> nodes =
        Arrays.stream(feNodes.split(",")).map(String::trim).collect(Collectors.toList());
    Collections.shuffle(nodes);
    return nodes;
  }

  @VisibleForTesting
  public static String randomBackend(DorisSinkConfig.DorisSinkOptions dorisConfig)
      throws DorisConnectorException {
    List<BackendV2.BackendRowV2> backends = getBackendsV2(dorisConfig);
    log.trace("Parse beNodes '{}'.", backends);
    if (backends == null || backends.isEmpty()) {
      log.error("argument '{}' is illegal, value is '{}'.", "beNodes", backends);
      String errMsg = String.format(ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
      throw new DorisConnectorException(errMsg);
    }
    Collections.shuffle(backends);
    BackendV2.BackendRowV2 backend = backends.get(0);
    return backend.getIp() + ":" + backend.getHttpPort();
  }

  public static String getBackend(DorisSinkConfig.DorisSinkOptions dorisConfig)
      throws DorisConnectorException {
    try {
      return randomBackend(dorisConfig);
    } catch (Exception e) {
      String errMsg = "Failed to get backend via " + dorisConfig.getFeNodes();
      throw new DorisConnectorException(errMsg, e);
    }
  }

  @Deprecated
  @VisibleForTesting
  static List<BackendRow> getBackends(DorisSinkConfig.DorisSinkOptions dorisConfig)
      throws DorisConnectorException {
    String feNodes = dorisConfig.getFeNodes();
    String feNode = randomEndpoint(feNodes);
    String beUrl = String.format(BASE_URL, feNode, BACKENDS);
    HttpGet httpGet = new HttpGet(beUrl);
    String response = send(dorisConfig, httpGet);
    log.info("Backend Info:{}", response);
    return parseBackend(response);
  }

  @Deprecated
  static List<BackendRow> parseBackend(String response) throws DorisConnectorException {
    Backend backend;
    try {
      backend = JsonUtils.readValue(response, Backend.class);
    } catch (JsonParseException e) {
      String errMsg = "Doris BE's response is not a json. res: " + response;
      log.error(errMsg, e);
      throw new DorisConnectorException(errMsg, e);
    } catch (JsonMappingException e) {
      String errMsg = "Doris BE's response cannot map to schema. res: " + response;
      log.error(errMsg, e);
      throw new DorisConnectorException(errMsg, e);
    } catch (IOException e) {
      String errMsg = "Parse Doris BE's response to json failed. res: " + response;
      log.error(errMsg, e);
      throw new DorisConnectorException(errMsg, e);
    }

    if (backend == null) {
      log.error(SHOULD_NOT_HAPPEN_MESSAGE);
      throw new DorisConnectorException(SHOULD_NOT_HAPPEN_MESSAGE);
    }
    List<BackendRow> backendRows =
        backend.getRows().stream().filter(BackendRow::getAlive).collect(Collectors.toList());
    log.debug("Parsing schema result is '{}'.", backendRows);
    return backendRows;
  }

  @VisibleForTesting
  public static List<BackendV2.BackendRowV2> getBackendsV2(
      DorisSinkConfig.DorisSinkOptions dorisConfig) throws DorisConnectorException {
    String feNodes = dorisConfig.getFeNodes();
    List<String> feNodeList = allEndpoints(feNodes);
    for (String feNode : feNodeList) {
      try {
        String beUrl = "http://" + feNode + BACKENDS_V2;
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(dorisConfig, httpGet);
        log.info("Backend Info:{}", response);
        return parseBackendV2(response);
      } catch (DorisConnectorException e) {
        log.info(
            "Doris FE node {} is unavailable: {}, Request the next Doris FE node",
            feNode,
            e.getMessage());
      }
    }
    String errMsg = "No Doris FE is available, please check configuration";
    throw new DorisConnectorException(errMsg);
  }

  static List<BackendV2.BackendRowV2> parseBackendV2(String response)
      throws DorisConnectorException {
    BackendV2 backend;
    try {
      backend = JsonUtils.readValue(response, BackendV2.class);
    } catch (JsonParseException e) {
      String errMsg = "Doris BE's response is not a json. res: " + response;
      log.error(errMsg, e);
      throw new DorisConnectorException(errMsg, e);
    } catch (JsonMappingException e) {
      String errMsg = "Doris BE's response cannot map to schema. res: " + response;
      log.error(errMsg, e);
      throw new DorisConnectorException(errMsg, e);
    } catch (IOException e) {
      String errMsg = "Parse Doris BE's response to json failed. res: " + response;
      log.error(errMsg, e);
      throw new DorisConnectorException(errMsg, e);
    }

    if (backend == null) {
      throw new DorisConnectorException(SHOULD_NOT_HAPPEN_MESSAGE);
    }
    List<BackendV2.BackendRowV2> backendRows = backend.getBackends();
    log.debug("Parsing schema result is '{}'.", backendRows);
    return backendRows;
  }
}
