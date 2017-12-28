package com.level11data.databricks.library;

import com.level11data.databricks.client.entities.libraries.LibraryFullStatusDTO;

public class LibraryStatus {
    public final LibraryInstallStatus InstallStatus;
    public final String[] Messages;
    public final boolean IsLibraryForAllClusters;

    public LibraryStatus(LibraryFullStatusDTO libraryFullStatusDTO) {
        InstallStatus = LibraryInstallStatus.valueOf(libraryFullStatusDTO.Status);
        Messages = libraryFullStatusDTO.Messages;
        IsLibraryForAllClusters = libraryFullStatusDTO.IsLibraryForAllClusters;
    }

}
