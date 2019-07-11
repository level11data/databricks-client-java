package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.*;
import com.level11data.databricks.cluster.ClusterLibrary;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.library.util.LibraryHelper;

public class CranLibrary extends AbstractPublishedLibrary {
    private final LibrariesClient _client;

    public final String PackageName;
    public final String RepoOverride;

    public CranLibrary(LibrariesClient client, String packageName) {
        super(client);
        _client = client;
        PackageName = packageName;
        RepoOverride = null;
    }

    public CranLibrary(LibrariesClient client, String packageName, String repo) {
        super(client);
        _client = client;
        PackageName = packageName;
        RepoOverride = repo;
    }

    public LibraryStatus getClusterStatus(InteractiveCluster cluster) throws LibraryConfigException {
        try {
            ClusterLibraryStatusesDTO libStatuses = _client.getClusterStatus(cluster.getId());

            //find library status for this library
            for (LibraryFullStatusDTO libStat : libStatuses.LibraryStatuses) {
                if(libStat.Library.Cran != null) {
                    if(libStat.Library.Cran.Package.equals(this.PackageName)) {
                        return new LibraryStatus(libStat);
                    }
                }
            }
        } catch (HttpException e) {
            throw new LibraryConfigException(e);
        }
        throw new LibraryConfigException("CRANLibrary " + this.PackageName +
                " Not Associated With Cluster Id " + cluster.getId());
    }

    public ClusterLibrary install(InteractiveCluster cluster) throws LibraryConfigException {
        try{
            _client.installLibraries(createLibraryRequest(cluster, LibraryHelper.createLibraryDTO(this)));
            return new ClusterLibrary(cluster, this);
        }catch(HttpException e) {
            throw new LibraryConfigException(e);
        }
    }

    public void uninstall(InteractiveCluster cluster) throws LibraryConfigException {
        try{
            _client.uninstallLibraries(createLibraryRequest(cluster, LibraryHelper.createLibraryDTO(this)));
        }catch(HttpException e) {
            throw new LibraryConfigException(e);
        }
    }

}
