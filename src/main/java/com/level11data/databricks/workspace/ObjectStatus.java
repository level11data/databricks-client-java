package com.level11data.databricks.workspace;

import com.level11data.databricks.client.entities.workspace.StatusResponseDTO;

public class ObjectStatus {

    public final String WorkspacePath;
    public final com.level11data.databricks.workspace.NotebookLanguage NotebookLanguage;
    public final ObjectType Type;

    public ObjectStatus(StatusResponseDTO statusResponseDTO) {
        WorkspacePath = statusResponseDTO.Path;

        Type = ObjectType.valueOf(statusResponseDTO.ObjectType);

        if(statusResponseDTO.Language != null) {
            NotebookLanguage = com.level11data.databricks.workspace.NotebookLanguage.valueOf(statusResponseDTO.Language);
        } else {
            NotebookLanguage = null;
        }
    }
}
