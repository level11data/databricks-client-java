package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryStatusesDTO;
import com.level11data.databricks.client.entities.libraries.LibraryFullStatusDTO;
import com.level11data.databricks.cluster.ClusterLibrary;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.library.util.LibraryHelper;

import java.net.URI;

public class EggLibrary extends AbstractPrivateLibrary {
    private final LibrariesClient _client;

    public EggLibrary(LibrariesClient client, URI uri) throws LibraryConfigException {
        super(client, uri);
        _client = client;
    }

    public LibraryStatus getClusterStatus(InteractiveCluster cluster) throws LibraryConfigException {
        try{
            ClusterLibraryStatusesDTO libStatuses = _client.getClusterStatus(cluster.getId());

            //find library status for this library
            for (LibraryFullStatusDTO libStat : libStatuses.LibraryStatuses) {
                if(libStat.Library.Egg != null) {
                    if(libStat.Library.Egg.equals(this.Uri.toString())) {
                        return new LibraryStatus(libStat);
                    }
                }
            }
        } catch(HttpException e) {
            throw new LibraryConfigException(e);
        }

        throw new LibraryConfigException("EggLibrary " + this.Uri.toString() +
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
        } catch(HttpException e) {
            throw new LibraryConfigException(e);
        }
    }
}
