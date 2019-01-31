package com.github.takemikami.databricks.workspacesync;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

@Slf4j
public class SyncCommand {

  private static void printHelp(Options options, Exception ex) {
    if (ex != null) {
      System.out.println("ERROR: " + ex.getMessage()); //NOPMD
    }
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("SyncCommand [upload|download|help] options", options);
  }

  private static void printHelp(Options options) {
    printHelp(options, null);
  }

  /**
   * command entry point.
   */
  public static void main(String[] args)
      throws ParseException, IOException, DatabricksClientException {
    // Prepare CommandLine Options
    Options options = new Options();
    options.addRequiredOption("h", "host", true, "Databricks host name");
    options.addRequiredOption("t", "token", true, "Databricks access token");
    options.addRequiredOption("l", "localdir", true, "Local directory path");
    options.addRequiredOption("r", "remotedir", true, "Remote directory path");
    options.addOption("e", "excludes", true, "Exclude file pattern");

    // Parse Options
    if (args.length == 0) {
      printHelp(options);
      return;
    }
    String subcmd = args[0];
    if (!"upload".equals(subcmd) && !"download".equals(subcmd)) {
      printHelp(options);
      return;
    }
    String[] opts = Arrays.copyOfRange(args, 1, args.length);
    CommandLine cmd;
    try {
      CommandLineParser parser = new DefaultParser();
      cmd = parser.parse(options, opts);
    } catch (MissingOptionException ex) {
      printHelp(options, ex);
      return;
    }

    String host = cmd.getOptionValue("host");
    String token = cmd.getOptionValue("token");
    String localdir = cmd.getOptionValue("localdir");
    String remotedir = cmd.getOptionValue("remotedir");
    String excludeString = cmd.getOptionValue("excludes");
    String[] excludes = new String[]{};
    if (excludeString != null) {
      excludes = excludeString.split(",");
    }

    LocalWorkspaceClient lcli = new LocalWorkspaceClient(localdir, remotedir);
    DatabricksWorkspaceClient cli = new DatabricksWorkspaceClient();
    cli.startSession(host, token);
    SyncService svc = new SyncService(cli, lcli);

    // Prompt
    Scanner scanner = new Scanner(System.in, "UTF-8");
    System.out.println("[" + subcmd + "] Remote:" + remotedir + ", Local: " + localdir); //NOPMD
    System.out.print("Start to process " + subcmd + "! Ready? [y/N]: "); //NOPMD
    String name = scanner.nextLine();
    if (name.charAt(0) != 'y' && name.charAt(0) != 'Y') {
      return;
    }

    // Execute
    switch (subcmd) {
      case "upload":
        svc.upload(remotedir, excludes, true);
        break;
      case "download":
        svc.download(remotedir, excludes);
        break;
      default:
        break;
    }
  }

}
