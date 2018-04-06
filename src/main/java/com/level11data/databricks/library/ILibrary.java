package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.cluster.ClusterLibrary;
import com.level11data.databricks.cluster.InteractiveCluster;

public interface ILibrary {

    LibraryStatus getClusterStatus(InteractiveCluster cluster) throws LibraryConfigException;

    ClusterLibrary install(InteractiveCluster cluster) throws HttpException;

    void uninstall(InteractiveCluster cluster) throws HttpException;
}
