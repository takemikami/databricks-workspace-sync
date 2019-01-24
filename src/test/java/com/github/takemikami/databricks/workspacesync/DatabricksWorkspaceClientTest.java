package com.github.takemikami.databricks.workspacesync;

import static junit.framework.TestCase.assertEquals;

import com.github.takemikami.databricks.workspacesync.WorkspaceObject.Language;
import com.github.takemikami.databricks.workspacesync.WorkspaceObject.ObjectType;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatabricksWorkspaceClientTest {

  private static final String DB_HOSTNAME = "xxxxxxxx.databricks.com";
  private static final String ACCESS_TOKEN = "xxxxxxxx";

  private DatabricksWorkspaceClient restClient;

  @Before
  public void setUp() {
    this.restClient = new DatabricksWorkspaceClient();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testParseExceptionResponse() throws Exception {
    String str
        = "{\"error_code\":\"RESOURCE_ALREADY_EXISTS\","
        + "\"message\":\"Path (/Users/xxx/dummy) already exists.\"}";
    DatabricksClientException ex = restClient.parseExceptionResponse(new RuntimeException(), str);

    assertEquals("RESOURCE_ALREADY_EXISTS", ex.getErrorCode());
    assertEquals("Path (/Users/xxx/dummy) already exists.", ex.getMessage());
  }

  @Test
  public void testParseListResponse() throws Exception {
    String str
        = "{\"objects\":[{\"object_type\":\"NOTEBOOK\","
        + "\"path\":\"/xxxx/foobar\",\"language\":\"PYTHON\"}]}\n";
    List<WorkspaceObject> lst = restClient.parseListResponse(str);

    assertEquals(1, lst.size());
    assertEquals("/xxxx/foobar", lst.get(0).getPath());
    assertEquals(ObjectType.NOTEBOOK, lst.get(0).getObjectType());
    assertEquals(Language.PYTHON, lst.get(0).getLanguage());
  }

  @Test
  public void testGetList() throws Exception {
    restClient.startSession(DB_HOSTNAME, ACCESS_TOKEN);

    String target = "/Users/xxxxx";
    // List<WorkspaceObject> lst = restClient.getList(target, false);
    // lst.forEach(x -> System.out.println(x));
  }

  @Test
  public void testExportObject() throws Exception {
    restClient.startSession(DB_HOSTNAME, ACCESS_TOKEN);

    String target = "/Users/xxxxx";
    // String rtn = restClient.exportObject(target);
    // System.out.println(rtn);
  }

  @Test
  public void testImportObject() throws Exception {
    restClient.startSession(DB_HOSTNAME, ACCESS_TOKEN);

    String target = "/Users/xxxx";
    // restClient.importObject(target, WorkspaceObject.Language.SCALA, false, "everything");
  }

}
