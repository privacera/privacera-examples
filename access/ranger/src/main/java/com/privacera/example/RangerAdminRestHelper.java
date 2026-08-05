package com.privacera.example;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Helper for Ranger Admin REST endpoints that are not exposed on {@link org.apache.ranger.RangerClient}.
 * Uses JDK {@link HttpClient} with Basic authentication (same approach as {@link RangerSecurityZoneDemo}).
 * Kerberos and Ranger SSL config-file loading are not supported here; use {@code RangerClient} for those.
 */
public final class RangerAdminRestHelper {

  private static final String AUTH_KERBEROS = "kerberos";

  private final HttpClient httpClient;
  private final String baseUrl;
  private final String basicAuthHeader;

  /**
   * Creates a REST helper using JDK {@link HttpClient} and Basic authentication.
   *
   * @param hostName Ranger Admin base URL
   * @param authType authentication type; only non-Kerberos (for example {@code basic}) is supported
   * @param userName username for Basic auth
   * @param password password for Basic auth
   * @param configFile unused; kept for call-site compatibility with {@code RangerClient} CLI args
   */
  public RangerAdminRestHelper(String hostName, String authType, String userName, String password, String configFile) {
    if (AUTH_KERBEROS.equalsIgnoreCase(authType)) {
      throw new IllegalArgumentException(
          "RangerAdminRestHelper supports Basic auth only. Kerberos is not supported for direct Admin REST calls; "
              + "use RangerClient for Kerberos-authenticated APIs.");
    }
    // configFile is intentionally unused: JDK HttpClient uses the JVM default SSL truststore.
    this.baseUrl = trimTrailingSlash(hostName);
    this.basicAuthHeader = "Basic " + Base64.getEncoder()
        .encodeToString((userName + ":" + password).getBytes(StandardCharsets.UTF_8));
    this.httpClient = HttpClient.newHttpClient();
  }

  /**
   * Performs an authenticated GET request against Ranger Admin.
   * Accepts both application/json and text/json so endpoints like policy exportJson work.
   */
  public String get(String relativePath, Map<String, String> queryParams) throws Exception {
    URI uri = buildUri(relativePath, queryParams);
    HttpRequest request = HttpRequest.newBuilder(uri)
        .header("Authorization", basicAuthHeader)
        .header("Accept", "application/json, text/json")
        .GET()
        .build();
    return send(request, "GET", relativePath);
  }

  /**
   * Uploads a file via multipart POST for endpoints such as policy import.
   */
  public String postMultipart(String relativePath, Map<String, String> queryParams, String fileFieldName,
      String fileName, String fileContent, String contentType) throws Exception {
    String boundary = "----RangerExampleBoundary" + System.currentTimeMillis();
    String multipartHeader = "--" + boundary + "\r\n"
        + "Content-Disposition: form-data; name=\"" + fileFieldName + "\"; filename=\"" + fileName + "\"\r\n"
        + "Content-Type: " + contentType + "\r\n\r\n";
    String multipartFooter = "\r\n--" + boundary + "--\r\n";
    byte[] body = (multipartHeader + fileContent + multipartFooter).getBytes(StandardCharsets.UTF_8);

    URI uri = buildUri(relativePath, queryParams);
    HttpRequest request = HttpRequest.newBuilder(uri)
        .header("Authorization", basicAuthHeader)
        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .build();
    return send(request, "POST", relativePath);
  }

  /**
   * Builds a URL-encoded query string from the given parameters.
   */
  public static String buildQueryString(Map<String, String> queryParams) {
    if (queryParams == null || queryParams.isEmpty()) {
      return "";
    }
    StringBuilder queryBuilder = new StringBuilder();
    for (Map.Entry<String, String> queryEntry : queryParams.entrySet()) {
      if (queryBuilder.length() > 0) {
        queryBuilder.append('&');
      }
      queryBuilder.append(URLEncoder.encode(queryEntry.getKey(), StandardCharsets.UTF_8));
      queryBuilder.append('=');
      queryBuilder.append(URLEncoder.encode(queryEntry.getValue(), StandardCharsets.UTF_8));
    }
    return queryBuilder.toString();
  }

  private URI buildUri(String relativePath, Map<String, String> queryParams) {
    String path = relativePath.startsWith("/") ? relativePath : "/" + relativePath;
    String query = buildQueryString(queryParams);
    String fullUrl = query.isEmpty() ? baseUrl + path : baseUrl + path + "?" + query;
    return URI.create(fullUrl);
  }

  private String send(HttpRequest request, String method, String path) throws IOException, InterruptedException {
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    int status = response.statusCode();
    String body = response.body() == null ? "" : response.body();
    if (status < 200 || status >= 300) {
      throw new IOException(method + " " + path + " failed with status " + status + ": " + body);
    }
    return body;
  }

  private static String trimTrailingSlash(String hostName) {
    if (hostName == null || hostName.isEmpty()) {
      throw new IllegalArgumentException("hostName is required");
    }
    return hostName.endsWith("/") ? hostName.substring(0, hostName.length() - 1) : hostName;
  }
}
