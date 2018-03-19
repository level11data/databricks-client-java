package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryRequestDTO;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryStatusesDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.LibraryFullStatusDTO;
import com.level11data.databricks.cluster.InteractiveCluster;
import java.net.URI;

public class JarLibrary extends PrivateLibrary {
    private final LibrariesClient _client;

    public JarLibrary(LibrariesClient client, URI uri) throws LibraryConfigException {
        super(client, uri);
        _client = client;
    }

    public LibraryStatus getClusterStatus(InteractiveCluster cluster) throws HttpException, LibraryConfigException {
        ClusterLibraryStatusesDTO libStatuses = _client.getClusterStatus(cluster.Id);

        //find library status for this library
        for (LibraryFullStatusDTO libStat : libStatuses.LibraryStatuses) {
            if(libStat.Library.Jar != null) {
                if(libStat.Library.Jar.equals(this.Uri.toString())) {
                    return new LibraryStatus(libStat);
                }
            }
        }
        throw new LibraryConfigException("Jar Library " + this.Uri.toString() +
                " Not Associated With Cluster Id " + cluster.Id);
    }

    public LibraryDTO createLibraryDTO() {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = this.Uri.toString();
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
