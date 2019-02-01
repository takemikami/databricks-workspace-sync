package com.github.takemikami.databricks.workspacesync;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.github.takemikami.databricks.workspacesync.WorkspaceObject.Language;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SyncServiceTest {

  private SyncService svc;

  @Mock
  private DatabricksWorkspaceClient client;

  @Mock
  private LocalWorkspaceClient localCli;

  @Before
  public void setUp() {
    svc = new SyncService(client, localCli);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testDownload() throws Exception {
    List<WorkspaceObject> lst = new LinkedList<>();
    lst.add(new WorkspaceObject("/dir1/dir2", "DIRECTORY", ""));
    lst.add(new WorkspaceObject("/dir1/dir2/file1", "NOTEBOOK", "SCALA"));
    doReturn(lst).when(client).getList("/dir1", true);
    doReturn("body string").when(client).exportObject("/dir1/dir2/file1");

    svc.download("/dir1", null);

    verify(localCli)
        .writeObject("/dir1/dir2/file1", WorkspaceObject.Language.SCALA, true, "body string");
  }

  @Test
  public void testDownloadExcludes() throws Exception {
    List<WorkspaceObject> lst = new LinkedList<>();
    lst.add(new WorkspaceObject("/dir1/dir2", "DIRECTORY", ""));
    lst.add(new WorkspaceObject("/dir1/dir2/file1", "NOTEBOOK", "SCALA"));
    lst.add(new WorkspaceObject("/dir1/dir2/exfile1", "NOTEBOOK", "SCALA"));
    doReturn(lst).when(client).getList("/dir1", true);
    doReturn("body string").when(client).exportObject("/dir1/dir2/file1");
    doReturn("body string ex").when(client).exportObject("/dir1/dir2/exfile1");

    svc.download("/dir1", new String[]{"*/ex*"});

    verify(localCli)
        .writeObject("/dir1/dir2/file1", WorkspaceObject.Language.SCALA, true, "body string");
    verify(localCli, never())
        .writeObject("/dir1/dir2/exfile1", WorkspaceObject.Language.SCALA, true, "body string ex");
  }

  @Test
  public void testUpload() throws Exception {
    List<WorkspaceObject> lst = new LinkedList<>();
    lst.add(new WorkspaceObject("/dir1/dir2/file1", "NOTEBOOK", "SCALA"));
    doReturn(lst).when(localCli).getList("/dir1");
    doReturn("body string").when(localCli).readObject("/dir1/dir2/file1", Language.SCALA);

    svc.upload("/dir1", null, true);

    verify(client)
        .importObject("/dir1/dir2/file1", WorkspaceObject.Language.SCALA, true, "body string");
  }

  @Test
  public void testBodyFilterUpload() throws Exception {
    String original = "this is python # NODBSYNC";
    String expect = "# this is python # NODBSYNC";
    assertEquals(expect, svc.filterBodyUpload(original));

    original = "this is scala // NODBSYNC";
    expect = "// this is scala // NODBSYNC";
    assertEquals(expect, svc.filterBodyUpload(original));

    original = "nothing to do";
    expect = "nothing to do";
    assertEquals(expect, svc.filterBodyUpload(original));
  }

  @Test
  public void testBodyFilterDownload() throws Exception {
    String original = "# this is python # NODBSYNC";
    String expect = "this is python # NODBSYNC";
    assertEquals(expect, svc.filterBodyDownload(original));

    original = "// this is scala // NODBSYNC";
    expect = "this is scala // NODBSYNC";
    assertEquals(expect, svc.filterBodyDownload(original));

    original = "nothing to do";
    expect = "nothing to do";
    assertEquals(expect, svc.filterBodyDownload(original));
  }
}
