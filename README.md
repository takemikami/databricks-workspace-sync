Databricks Workspace Sync Tool
---

Notebooks synchronized tool between Local and Remote Databricks workspace.

This tool provide following features.

- download noteoboks from databricks to local.
- upload noteoboks to databricks from local.

## Getting Started

Generate access token of your databricks account.

see. https://docs.databricks.com/api/latest/authentication.html#generate-a-token

Build this tool by gradle.

```
$ gradle shadowJar
```

Run download command.

```
$ java -jar build/libs/databricks-workspace-sync-all.jar download -h <your databricks host> -t <your token> -l <local directory> -r <remote directory> -e <exclucde file pattern>
```

Run upload command.

```
$ java -jar build/libs/databricks-workspace-sync-all.jar upload -h <your databricks host> -t <your token> -l <local directory> -r <remote directory> -e <exclucde file pattern>
```

- your databricks host: ex. 'community.cloud.databricks.com'
- your token: string of generated token
- local directory: ex '/tmp'
- remote directory: ex '/Users/you@domain/folder1'
- exclude file pattern (optional): ex '/tmp/*'