package com.github.takemikami.databricks.workspacesync;

import com.github.takemikami.databricks.workspacesync.WorkspaceObject.ObjectType;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SyncService {

  private DatabricksWorkspaceClient cli;
  private LocalWorkspaceClient lcli;

  /**
   * constructor.
   */
  public SyncService(DatabricksWorkspaceClient cli, LocalWorkspaceClient lcli) {
    this.cli = cli;
    this.lcli = lcli;
  }

  /**
   * download workspace object to local workspace.
   */
  @SuppressWarnings("PMD")
  public void download(String remotePath, String[] excludes)
      throws IOException, DatabricksClientException {
    cli.getList(remotePath, true).stream()
        .filter(e -> e.getObjectType() == ObjectType.NOTEBOOK)
        .forEach(f -> {
          try {
            String body = cli.exportObject(f.getPath());
            lcli.writeObject(f.getPath(), f.getLanguage(), true, body);
            System.out.println("download: " + f.getPath()); //NOPMD
          } catch (DatabricksClientException e) {
            log.warn("Download Error: ", e);
          } catch (IOException e) {
            log.warn("Download Error: ", e);
          }
        });
  }

  /**
   * upload workspace object from local workspace.
   */
  public void upload(String remotePath, String[] excludes, boolean overwrite) throws IOException {
    lcli.getList(remotePath)
        .forEach(f -> {
          try {
            String body = lcli.readObject(f.getPath(), f.getLanguage());
            cli.importObject(f.getPath(), f.getLanguage(), overwrite, body);
            System.out.println("upload: " + f.getPath()); //NOPMD
          } catch (DatabricksClientException e) {
            log.warn("Download Error: ", e);
          } catch (IOException e) {
            log.warn("Download Error: ", e);
          }
        });
  }

}
