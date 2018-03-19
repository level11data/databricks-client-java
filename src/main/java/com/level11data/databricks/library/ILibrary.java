package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.cluster.InteractiveCluster;

public interface ILibrary {

    LibraryDTO createLibraryDTO();

    void uninstall(InteractiveCluster cluster) throws HttpException;
}
