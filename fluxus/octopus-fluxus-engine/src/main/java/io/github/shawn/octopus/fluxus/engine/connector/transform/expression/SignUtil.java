package io.github.shawn.octopus.fluxus.engine.connector.transform.expression;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;

@Slf4j
public final class SignUtil {
  public static final String HASH_TYPE_SHA256 = "SHA256";

  public static final String APP_PASSWORD = "hakPIC@2023";

  private SignUtil() {}

  private static SignUtil signUtil;

  public static synchronized SignUtil getInstance() {
    if (signUtil == null) {
      signUtil = new SignUtil();
    }
    return signUtil;
  }

  /**
   * 创建签名字符串 保证线程安全
   *
   * @param parameters 签名的 map
   * @param appSecret 签名的 appsecret
   * @param timestamp 签名的时间戳
   * @param hashType 签名的加密类型
   * @return 签名
   */
  public synchronized String createSign(
      Map<String, Object> parameters, String appSecret, String timestamp, String hashType) {
    StringBuilder sb = new StringBuilder();
    sb.append(appSecret).append(timestamp);
    // 所有参与传参的参数按照 accsii 排序（升序）
    if (!(parameters == null || parameters.isEmpty())) {
      parameters
          .entrySet()
          .stream()
          // 过滤 sign 自身参数
          .filter(
              paramEntry ->
                  (paramEntry.getValue() != null)
                      && !paramEntry.getKey().equals("sign")
                      && !paramEntry.getKey().equals("key"))
          .sorted(Map.Entry.comparingByKey())
          .forEach(
              paramEntry ->
                  sb.append('&')
                      .append(paramEntry.getKey())
                      .append("=")
                      .append(paramEntry.getValue()));
    }
    if (HASH_TYPE_SHA256.equals(hashType)) {
      return DigestUtils.sha256Hex(sb.toString()).toUpperCase();
    }
    return sb.toString();
  }
}
