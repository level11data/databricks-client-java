package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.*;
import com.level11data.databricks.cluster.InteractiveCluster;

public class PyPiLibrary extends PublishedLibrary {
    private final LibrariesClient _client;

    public final String PackageName;
    public final String RepoOverride;

    public PyPiLibrary(LibrariesClient client, String packageName) {
        super(client);
        _client = client;
        PackageName = packageName;
        RepoOverride = null;
    }

    public PyPiLibrary(LibrariesClient client, String packageName, String repo) {
        super(client);
        _client = client;
        PackageName = packageName;
        RepoOverride = repo;
    }

    public LibraryStatus getClusterStatus(InteractiveCluster cluster) throws HttpException, LibraryConfigException {
        ClusterLibraryStatusesDTO libStatuses = _client.getClusterStatus(cluster.Id);

        //find library status for this library
        for (LibraryFullStatusDTO libStat : libStatuses.LibraryStatuses) {
            if(libStat.Library.PyPi != null) {
                if(libStat.Library.PyPi.Package.equals(this.PackageName)) {
                    return new LibraryStatus(libStat);
                }
            }
        }
        throw new LibraryConfigException("PyPi Library " + this.PackageName +
                " Not Associated With Cluster Id " + cluster.Id);
    }

    public LibraryDTO createLibraryDTO() {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO pyPiLibrary = new PythonPyPiLibraryDTO();
        pyPiLibrary.Package = this.PackageName;
        pyPiLibrary.Repo = this.RepoOverride;
        libraryDTO.PyPi = pyPiLibrary;
        return libraryDTO;
    }

    public void uninstall(InteractiveCluster cluster) throws HttpException {
        ClusterLibraryRequestDTO clusterLibraryRequest = new ClusterLibraryRequestDTO();
        clusterLibraryRequest.ClusterId = cluster.Id;

        LibraryDTO[] libraries = new LibraryDTO[1];
        libraries[0] = this.createLibraryDTO();
        clusterLibraryRequest.Libraries = libraries;

        _client.uninstallLibraries(clusterLibraryRequest);
    }
}
