package com.github.takemikami.databricks.workspacesync;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class WorkspaceObject {

  public enum ObjectType {
    NOTEBOOK,
    DIRECTORY,
    LIBRARY,
  }

  public enum Language {
    SCALA,
    PYTHON,
    SQL,
    R,
  }

  /**
   * constructor from string of path, objectType, language.
   */
  public WorkspaceObject(String path, String objectType, String language) {
    this.setPath(path);

    if (objectType != null) {
      switch (objectType) {
        case "NOTEBOOK":
          this.setObjectType(ObjectType.NOTEBOOK);
          break;
        case "DIRECTORY":
          this.setObjectType(ObjectType.DIRECTORY);
          break;
        case "LIBRARY":
          this.setObjectType(ObjectType.LIBRARY);
          break;
        default:
          break;
      }
    }

    if (language != null) {
      switch (language) {
        case "SCALA":
          this.setLanguage(Language.SCALA);
          break;
        case "PYTHON":
          this.setLanguage(Language.PYTHON);
          break;
        case "SQL":
          this.setLanguage(Language.SQL);
          break;
        case "R":
          this.setLanguage(Language.R);
          break;
        default:
          break;
      }
    }
  }

  private String path;
  private ObjectType objectType;
  private Language language;
}
