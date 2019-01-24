package com.github.takemikami.databricks.workspacesync;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;

import com.github.takemikami.databricks.workspacesync.WorkspaceObject.Language;
import org.junit.Test;

public class LocalWorkspaceClientTest {

  @Test
  public void testTransLocalFileName() throws Exception {
    String parentPath = "/tmp";
    String remotePath = "/Users/xxxx@xxxx/foo";
    LocalWorkspaceClient lcli = new LocalWorkspaceClient(parentPath, remotePath);

    assertEquals("/tmp/test.py",
        lcli.transLocalFileName("/Users/xxxx@xxxx/foo/test", Language.PYTHON));
    assertEquals("/tmp/test",
        lcli.transLocalFileName("/Users/xxxx@xxxx/foo/test", null));
  }

  @Test
  public void testTransRemoteFile() throws Exception {
    String parentPath = "/tmp";
    String remotePath = "/Users/xxxx@xxxx/foo";
    LocalWorkspaceClient lcli = new LocalWorkspaceClient(parentPath, remotePath);

    WorkspaceObject obj1 = lcli.transRemoteFile("/tmp/test.py");
    assertEquals("/Users/xxxx@xxxx/foo/test", obj1.getPath());
    assertEquals(Language.PYTHON, obj1.getLanguage());

    WorkspaceObject obj2 = lcli.transRemoteFile("/tmp/test");
    assertEquals("/Users/xxxx@xxxx/foo/test", obj2.getPath());
    assertNull(obj2.getLanguage());
  }

  @Test
  public void testGetList() throws Exception {
    //    String parentPath = "/tmp";
    //    String remotePath = "/Users/xxxx@xxxx/foo";
    //    LocalWorkspaceClient lcli = new LocalWorkspaceClient(parentPath, remotePath);
    //    lcli.getList("/Users/xxxx@xxxx/foo/scala");
  }

  @Test
  public void testRead() throws Exception {
    //    String parentPath = "/tmp";
    //    String remotePath = "/Users/xxxx@xxxx/foo";
    //    LocalWorkspaceClient lcli = new LocalWorkspaceClient(parentPath, remotePath);
    //    System.out.println(lcli.readObject("/Users/xxxx@xxxx/foo", Language.SCALA));
  }

  @Test
  public void testWrite() throws Exception {
    //    String parentPath = "/tmp";
    //    String remotePath = "/Users/xxxx@xxxx/foo";
    //    LocalWorkspaceClient lcli = new LocalWorkspaceClient(parentPath, remotePath);
    //    lcli.writeObject("/Users/xxxx@xxxx/foo/bar", Language.SCALA, true, "bodybody");
  }

}
