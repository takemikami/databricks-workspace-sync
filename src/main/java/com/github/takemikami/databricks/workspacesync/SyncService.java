package com.github.takemikami.databricks.workspacesync;

import com.github.takemikami.databricks.workspacesync.WorkspaceObject.ObjectType;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pfsw.text.StringPattern;

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

  private List<StringPattern> createExcludePatterns(String... excludes) {
    if (excludes == null) {
      return new LinkedList<>();
    }
    return Arrays.stream(excludes)
        .map(StringPattern::create)
        .collect(Collectors.toList());
  }

  /**
   * download workspace object to local workspace.
   */
  @SuppressWarnings("PMD")
  public void download(String remotePath, String[] excludes)
      throws IOException, DatabricksClientException {
    List<StringPattern> mt = createExcludePatterns(excludes);
    cli.getList(remotePath, true).stream()
        .filter(e -> e.getObjectType() == ObjectType.NOTEBOOK)
        .filter(e -> mt.stream()
            .noneMatch(re -> re.matches(e.getPath().substring(remotePath.length()))))
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
    List<StringPattern> mt = createExcludePatterns(excludes);
    lcli.getList(remotePath)
        .stream()
        .filter(e -> mt.stream()
            .noneMatch(re -> re.matches(e.getPath().substring(remotePath.length()))))
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
