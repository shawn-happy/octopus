package io.github.shawn.octopus.data.fluxus.engine.connector.sink.doris.write;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.github.shawn.octopus.data.fluxus.engine.connector.sink.doris.DorisConstants.DORIS_DELETE_SIGN;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

public class HttpPutBuilder {
  private String url;
  private final Map<String, String> header;
  private HttpEntity httpEntity;

  public HttpPutBuilder() {
    header = new HashMap<>();
  }

  public HttpPutBuilder setUrl(String url) {
    this.url = url;
    return this;
  }

  public HttpPutBuilder addCommonHeader() {
    header.put(HttpHeaders.EXPECT, "100-continue");
    header.put("Content-Type", "text/plain");
    return this;
  }

  public HttpPutBuilder addHiddenColumns(boolean add) {
    if (add) {
      header.put("hidden_columns", DORIS_DELETE_SIGN);
    }
    return this;
  }

  public HttpPutBuilder enable2PC() {
    header.put("two_phase_commit", "true");
    return this;
  }

  public HttpPutBuilder baseAuth(String user, String password) {
    final String authInfo = user + ":" + password;
    byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
    header.put(HttpHeaders.AUTHORIZATION, "Basic " + new String(encoded));
    return this;
  }

  public HttpPutBuilder addTxnId(long txnID) {
    header.put("txn_id", String.valueOf(txnID));
    return this;
  }

  public HttpPutBuilder commit() {
    header.put("txn_operation", "commit");
    return this;
  }

  public HttpPutBuilder abort() {
    header.put("txn_operation", "abort");
    return this;
  }

  public HttpPutBuilder setEntity(HttpEntity httpEntity) {
    this.httpEntity = httpEntity;
    return this;
  }

  public HttpPutBuilder setEmptyEntity() {
    try {
      this.httpEntity = new StringEntity("");
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    return this;
  }

  public HttpPutBuilder addProperties(Properties properties) {
    properties.forEach((key, value) -> header.put(String.valueOf(key), String.valueOf(value)));
    return this;
  }

  public HttpPutBuilder setLabel(String label) {
    header.put("label", label);
    return this;
  }

  public HttpPut build() {
    checkNotNull(url);
    checkNotNull(httpEntity);
    HttpPut put = new HttpPut(url);
    header.forEach(put::setHeader);
    put.setEntity(httpEntity);
    return put;
  }
}
