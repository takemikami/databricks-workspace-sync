package com.github.takemikami.databricks.workspacesync;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = false)
public class DatabricksClientException extends Exception {

  private String errorCode;
  private String message;
  private Exception throwedException;
}
