package com.github.takemikami.databricks.workspacesync;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.takemikami.databricks.workspacesync.WorkspaceObject.ObjectType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksWorkspaceClient {

  private Client client;
  private String dbHostName;
  private MultivaluedMap<String, Object> headers;

  /**
   * start databricks rest api client session.
   */
  public void startSession(String dbHostName, String accessToken) {
    this.dbHostName = dbHostName;
    headers = new MultivaluedHashMap<>();
    headers.putSingle("Authorization", "Bearer " + accessToken);
    client = ClientBuilder.newClient();
  }

  /**
   * close databricks rest api client session.
   */
  public void closeSession() {
    client.close();
    client = null;
  }

  private Builder createRequestBuilder(String apiPath, Map<String, String> queryParams) {
    WebTarget target = client.target("https://" + this.dbHostName).path(apiPath);
    for (Map.Entry<String, String> qp : queryParams.entrySet()) {
      target = target.queryParam(qp.getKey(), qp.getValue());
    }
    return target.request().headers(this.headers);
  }

  DatabricksClientException parseExceptionResponse(Exception ex, String resp) throws IOException {
    DatabricksClientException rtn = new DatabricksClientException();
    Map<String, String> jsonObj;
    try {
      ObjectMapper mapper = new ObjectMapper();
      jsonObj = mapper.readValue(resp, new TypeReference<HashMap<String, String>>() {
      });
      rtn.setErrorCode(jsonObj.get("error_code"));
      rtn.setMessage(jsonObj.get("message"));
    } catch (IOException e) {
      throw e;
    }
    return rtn;
  }

  List<WorkspaceObject> parseListResponse(String resp) throws IOException {
    Map<String, List<Map<String, String>>> jsonObj;
    try {
      ObjectMapper mapper = new ObjectMapper();
      jsonObj = mapper.readValue(resp,
          new TypeReference<HashMap<String, ArrayList<HashMap<String, Object>>>>() {
          });
    } catch (IOException e) {
      throw e;
    }

    if (jsonObj.get("objects") == null) {
      return new LinkedList<>();
    }
    return jsonObj.get("objects").stream().map(e ->
        new WorkspaceObject(e.get("path"), e.get("object_type"), e.get("language"))
    ).collect(Collectors.toList());
  }

  /**
   * get file list from databricks workspace.
   */
  public List<WorkspaceObject> getList(String path, boolean recursive)
      throws DatabricksClientException, IOException {
    if (client == null) {
      throw new RuntimeException("Databricks REST API Session is not enable.");
    }
    String apiPath = "/api/2.0/workspace/list";
    Map<String, String> queryParams = new ConcurrentHashMap<>();
    queryParams.put("path", path);
    Builder builder = createRequestBuilder(apiPath, queryParams);

    String result = null;
    try {
      result = builder.get(String.class);
    } catch (ClientErrorException ex) {
      String resp = ex.getResponse().readEntity(String.class);
      throw parseExceptionResponse(ex, resp);
    }
    List<WorkspaceObject> lst = parseListResponse(result);

    if (recursive) {
      List<WorkspaceObject> subList = new LinkedList<>();
      for (WorkspaceObject e : lst) {
        if (e.getObjectType() == ObjectType.DIRECTORY) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException ex) {
            log.warn("thread sleep is fail.", ex);
          }
          subList.addAll(this.getList(e.getPath(), true));
        }
      }
      lst.addAll(subList);
    }

    return lst;
  }

  /**
   * export file from databricks workspace.
   */
  public String exportObject(String path)
      throws DatabricksClientException, IOException {
    if (client == null) {
      throw new RuntimeException("Databricks REST API Session is not enable.");
    }
    String apiPath = "/api/2.0/workspace/export";
    Map<String, String> queryParams = new ConcurrentHashMap<>();
    queryParams.put("path", path);
    queryParams.put("direct_download", "true");
    Builder builder = createRequestBuilder(apiPath, queryParams);

    String result = null;
    try {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {
        log.warn("thread sleep is fail.", ex);
      }
      result = builder.get(String.class);
    } catch (ClientErrorException ex) {
      String resp = ex.getResponse().readEntity(String.class);
      throw parseExceptionResponse(ex, resp);
    }
    return result;
  }

  /**
   * import file to databricks workspace.
   */
  public void importObject(String path,
      WorkspaceObject.Language language, boolean overwrite, String body)
      throws DatabricksClientException, IOException {
    if (client == null) {
      throw new RuntimeException("Databricks REST API Session is not enable.");
    }
    String apiPath = "/api/2.0/workspace/import";
    Builder builder = createRequestBuilder(apiPath, new HashMap<>());

    Map<String, String> formParams = new ConcurrentHashMap<>();
    String encodedBody = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
    formParams.put("path", path);
    formParams.put("language", language.toString());
    formParams.put("content", encodedBody);
    formParams.put("format", "SOURCE");
    formParams.put("overwrite", overwrite ? "true" : "false");

    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(formParams);
    Entity entity = Entity.entity(json, MediaType.APPLICATION_JSON_TYPE);

    try {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {
        log.warn("thread sleep is fail.", ex);
      }
      builder.post(entity, String.class);
    } catch (ClientErrorException ex) {
      String resp = ex.getResponse().readEntity(String.class);
      throw parseExceptionResponse(ex, resp);
    }
  }
}
