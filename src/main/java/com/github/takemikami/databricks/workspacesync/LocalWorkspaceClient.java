package com.github.takemikami.databricks.workspacesync;

import com.github.takemikami.databricks.workspacesync.WorkspaceObject.Language;
import com.github.takemikami.databricks.workspacesync.WorkspaceObject.ObjectType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalWorkspaceClient {

  private static Map<Language, String> languageExtension = new ConcurrentHashMap<>();

  static {
    languageExtension.put(WorkspaceObject.Language.SCALA, ".scala");
    languageExtension.put(WorkspaceObject.Language.PYTHON, ".py");
    languageExtension.put(WorkspaceObject.Language.SQL, ".sql");
    languageExtension.put(WorkspaceObject.Language.R, ".r");
  }

  private String parentPath;
  private String remotePath;

  /**
   * constructor.
   */
  public LocalWorkspaceClient(String parentPath, String remotePath) {
    this.parentPath = parentPath;
    this.remotePath = remotePath;
  }

  private String transLocalFileName(String path) {
    return transLocalFileName(path, null);
  }

  String transLocalFileName(String path, WorkspaceObject.Language language) {
    StringBuilder remoteFilename = new StringBuilder(path);
    if (language != null) {
      remoteFilename.append(languageExtension.getOrDefault(language, ""));
    }
    return parentPath + remoteFilename.toString().substring(remotePath.length());
  }

  WorkspaceObject transRemoteFile(String path) {
    String objPath = remotePath + path.substring(parentPath.length());
    String language = null;
    for (Map.Entry<Language, String> ext : languageExtension.entrySet()) {
      if (path.endsWith(ext.getValue())) {
        language = ext.getKey().toString();
        objPath = objPath.substring(0, objPath.length() - ext.getValue().length());
        break;
      }
    }
    return new WorkspaceObject(objPath, ObjectType.NOTEBOOK.toString(), language);
  }

  /**
   * get file list from local workspace.
   */
  public List<WorkspaceObject> getList(String path) throws IOException {
    String localPath = transLocalFileName(path);
    return Files.walk(Paths.get(localPath))
        .filter(e -> languageExtension.values()
            .stream().anyMatch(ext -> e.toString().endsWith(ext)))
        .filter(e -> Files.isRegularFile(e))
        .map(e -> transRemoteFile(e.toString()))
        .collect(Collectors.toList());
  }

  /**
   * read workspace object from local file.
   */
  public String readObject(String path, WorkspaceObject.Language language) throws IOException {
    String localName = transLocalFileName(path, language);
    try (BufferedReader fr = Files.newBufferedReader(Paths.get(localName))) {
      return fr.lines().collect(Collectors.joining("\n"));
    }
  }

  /**
   * write workspace object to local file.
   */
  public void writeObject(String path,
      WorkspaceObject.Language language, boolean overwrite, String body) throws IOException {
    String localName = transLocalFileName(path, language);
    Path parentDir = Paths.get(localName).getParent();
    if (parentDir != null && !Files.exists(parentDir)) {
      Files.createDirectories(parentDir);
    }
    try (Writer fw = Files.newBufferedWriter(Paths.get(localName))) {
      fw.write(body);
    }
  }

}

