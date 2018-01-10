package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryStatusesDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.LibraryFullStatusDTO;
import com.level11data.databricks.client.entities.libraries.MavenLibraryDTO;
import com.level11data.databricks.cluster.InteractiveCluster;

public class MavenLibrary extends PublishedLibrary {
    private final LibrariesClient _client;

    public final String Coordinates;
    public final String RepoOverride;
    public final String[] DependencyExclusions;

    public MavenLibrary(LibrariesClient client, String coordinates) {
        super();
        _client = client;
        Coordinates = coordinates;
        RepoOverride = null;
        DependencyExclusions = null;
    }

    public MavenLibrary(LibrariesClient client, String coordinates, String repo) {
        super();
        _client = client;
        Coordinates = coordinates;
        RepoOverride = repo;
        DependencyExclusions = null;
    }

    public MavenLibrary(LibrariesClient client, String coordinates, String repo, String[] exclusions) {
        super();
        _client = client;
        Coordinates = coordinates;
        RepoOverride = repo;
        DependencyExclusions = exclusions;
    }

    public MavenLibrary(LibrariesClient client, String coordinates, String[] exclusions) {
        super();
        _client = client;
        Coordinates = coordinates;
        RepoOverride = null;
        DependencyExclusions = exclusions;
    }

    public LibraryStatus getClusterStatus(InteractiveCluster cluster) throws HttpException, LibraryConfigException {
        ClusterLibraryStatusesDTO libStatuses = _client.getClusterStatus(cluster.Id);

        //find library status for this library
        for (LibraryFullStatusDTO libStat : libStatuses.LibraryStatuses) {
            if(libStat.Library.Maven != null) {
                if(libStat.Library.Maven.Coordinates.equals(this.Coordinates)) {
                    return new LibraryStatus(libStat);
                }
            }
        }
        throw new LibraryConfigException("Maven Library " + this.Coordinates +
                " Not Associated With Cluster Id " + cluster.Id);
    }

    public LibraryDTO createLibraryDTO() {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenLibraryDTO = new MavenLibraryDTO();
        mavenLibraryDTO.Coordinates = this.Coordinates;
        mavenLibraryDTO.Repo = this.RepoOverride;
        mavenLibraryDTO.Exclusions = this.DependencyExclusions;
        libraryDTO.Maven = mavenLibraryDTO;
        return libraryDTO;
    }
}
