package com.level11data.databricks.workspace.util;

import com.level11data.databricks.workspace.NotebookLanguage;

public class RemoteNotebook {
    public final NotebookLanguage Language;
    public final byte[] SourceCode;

    RemoteNotebook(NotebookLanguage lang, byte[] bytes) {
        Language = lang;
        SourceCode = bytes;
    }
}
