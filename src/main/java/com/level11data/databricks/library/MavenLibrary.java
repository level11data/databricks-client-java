package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.*;
import com.level11data.databricks.cluster.ClusterLibrary;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.library.util.LibraryHelper;

public class MavenLibrary extends AbstractPublishedLibrary {
    private final LibrariesClient _client;

    public final String Coordinates;
    public final String RepoOverride;
    public final String[] DependencyExclusions;

    public MavenLibrary(LibrariesClient client, String coordinates) {
        super(client);
        _client = client;
        Coordinates = coordinates;
        RepoOverride = null;
        DependencyExclusions = null;
    }

    public MavenLibrary(LibrariesClient client, String coordinates, String repo) {
        super(client);
        _client = client;
        Coordinates = coordinates;
        RepoOverride = repo;
        DependencyExclusions = null;
    }

    public MavenLibrary(LibrariesClient client, String coordinates, String repo, String[] exclusions) {
        super(client);
        _client = client;
        Coordinates = coordinates;
        RepoOverride = repo;
        DependencyExclusions = exclusions;
    }

    public MavenLibrary(LibrariesClient client, String coordinates, String[] exclusions) {
        super(client);
        _client = client;
        Coordinates = coordinates;
        RepoOverride = null;
        DependencyExclusions = exclusions;
    }

    public LibraryStatus getClusterStatus(InteractiveCluster cluster) throws LibraryConfigException {
        try {
            ClusterLibraryStatusesDTO libStatuses = _client.getClusterStatus(cluster.getId());

            //find library status for this library
            for (LibraryFullStatusDTO libStat : libStatuses.LibraryStatuses) {
                if(libStat.Library.Maven != null) {
                    if(libStat.Library.Maven.Coordinates.equals(this.Coordinates)) {
                        return new LibraryStatus(libStat);
                    }
                }
            }
        } catch(HttpException e) {
            throw new LibraryConfigException(e);
        }
        throw new LibraryConfigException("MavenLibrary " + this.Coordinates +
                " Not Associated With Cluster Id " + cluster.getId());
    }

    public ClusterLibrary install(InteractiveCluster cluster) throws LibraryConfigException {
        try{
            _client.installLibraries(createLibraryRequest(cluster, LibraryHelper.createLibraryDTO(this)));
            return new ClusterLibrary(cluster, this);
        }catch(HttpException e){
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
