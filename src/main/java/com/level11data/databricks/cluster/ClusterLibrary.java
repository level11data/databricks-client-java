package com.level11data.databricks.cluster;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.library.*;

public class ClusterLibrary {
    public final InteractiveCluster Cluster;
    public final Library Library;

    public ClusterLibrary(InteractiveCluster cluster, Library library) {
        Cluster = cluster;
        Library = library;
    }

    public LibraryStatus getLibraryStatus() throws HttpException, LibraryConfigException {
        if(Library instanceof JarLibrary) {
            return ((JarLibrary) Library).getClusterStatus(Cluster);
        } else if(Library instanceof EggLibrary) {
            return ((EggLibrary) Library).getClusterStatus(Cluster);
        } else if(Library instanceof MavenLibrary) {
            return ((MavenLibrary) Library).getClusterStatus(Cluster);
        } else if(Library instanceof PyPiLibrary) {
            return ((PyPiLibrary) Library).getClusterStatus(Cluster);
        } else if (Library instanceof CranLibrary) {
            return ((CranLibrary) Library).getClusterStatus(Cluster);
        } else {
            throw new LibraryConfigException("Unknown Library Type " + Library.getClass().getTypeName());
        }
    }

}
