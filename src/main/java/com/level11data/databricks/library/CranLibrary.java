package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryStatusesDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.LibraryFullStatusDTO;
import com.level11data.databricks.client.entities.libraries.RCranLibraryDTO;
import com.level11data.databricks.cluster.InteractiveCluster;

public class CranLibrary extends PublishedLibrary {
    private final LibrariesClient _client;

    public final String PackageName;
    public final String RepoOverride;

    public CranLibrary(LibrariesClient client, String packageName) {
        _client = client;
        PackageName = packageName;
        RepoOverride = null;
    }

    public CranLibrary(LibrariesClient client, String packageName, String repo) {
        _client = client;
        PackageName = packageName;
        RepoOverride = repo;
    }

    public LibraryStatus getClusterStatus(InteractiveCluster cluster) throws HttpException, LibraryConfigException {
        ClusterLibraryStatusesDTO libStatuses = _client.getClusterStatus(cluster.Id);

        //find library status for this library
        for (LibraryFullStatusDTO libStat : libStatuses.LibraryStatuses) {
            if(libStat.Library.Cran != null) {
                if(libStat.Library.Cran.Package.equals(this.PackageName)) {
                    return new LibraryStatus(libStat);
                }
            }
        }
        throw new LibraryConfigException("CRAN Library " + this.PackageName +
                " Not Associated With Cluster Id " + cluster.Id);
    }

    public LibraryDTO createLibraryDTO() {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranLibraryDTO = new RCranLibraryDTO();
        cranLibraryDTO.Package = this.PackageName;
        cranLibraryDTO.Repo = this.RepoOverride;
        libraryDTO.Cran = cranLibraryDTO;
        return libraryDTO;
    }

}
