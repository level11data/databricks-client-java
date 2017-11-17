package com.level11data.databricks.dbfs;

import com.level11data.databricks.client.entities.dbfs.FileInfoDTO;

public class DbfsFileInfo {
    public final String Path;
    public final boolean IsDir;
    public final long FileSize;

    public DbfsFileInfo(FileInfoDTO fileInfoDTO) {
        Path = fileInfoDTO.Path;
        IsDir = fileInfoDTO.IsDir;
        FileSize = fileInfoDTO.FileSize;
    }
}
