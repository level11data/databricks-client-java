package com.level11data.databricks.library;

import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryRequestDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.cluster.InteractiveCluster;

public abstract class AbstractLibrary implements Library {

    private final LibrariesClient _client;

    protected AbstractLibrary(LibrariesClient client) {
        _client = client;
    }

    protected ClusterLibraryRequestDTO createLibraryRequest(InteractiveCluster cluster, LibraryDTO libraryDTO) {
        ClusterLibraryRequestDTO clusterLibraryRequest = new ClusterLibraryRequestDTO();
        clusterLibraryRequest.ClusterId = cluster.getId();

        LibraryDTO[] libraries = new LibraryDTO[1];
        libraries[0] = libraryDTO;
        clusterLibraryRequest.Libraries = libraries;

        return clusterLibraryRequest;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof LibraryDTO && this instanceof JarLibrary) {
            if(((LibraryDTO) obj).Jar != null) {
                JarLibrary jarLibrary = (JarLibrary) this;
                if(((LibraryDTO) obj).Jar.equals(jarLibrary.Uri.toString())) {
                    return true;
                }
            }
        } else if(obj instanceof LibraryDTO && this instanceof EggLibrary) {
            if(((LibraryDTO) obj).Egg != null) {
                EggLibrary eggLibrary = (EggLibrary) this;
                if(((LibraryDTO) obj).Egg.equals(eggLibrary.Uri.toString())) {
                    return true;
                }
            }
        } else if(obj instanceof LibraryDTO && this instanceof MavenLibrary) {
            if(((LibraryDTO) obj).Maven != null) {
                MavenLibrary mavenLibrary = (MavenLibrary) this;
                if(((LibraryDTO) obj).Maven.Coordinates.equals(mavenLibrary.Coordinates)) {
                    return true;
                }
            }
        } else if(obj instanceof LibraryDTO && this instanceof PyPiLibrary) {
            if(((LibraryDTO) obj).PyPi != null) {
                PyPiLibrary pyPiLibrary = (PyPiLibrary) this;
                if(((LibraryDTO) obj).PyPi.Package.equals(pyPiLibrary.PackageName)) {
                    return true;
                }
            }
        } else if(obj instanceof LibraryDTO && this instanceof CranLibrary) {
            if (((LibraryDTO) obj).Cran != null) {
                CranLibrary cranLibrary = (CranLibrary) this;
                if (((LibraryDTO) obj).Cran.Package.equals(cranLibrary.PackageName)) {
                    return true;
                }
            }
        }

        if(obj instanceof JarLibrary && this instanceof JarLibrary) {
            if(((JarLibrary) obj).Uri.equals(((JarLibrary) this).Uri)) {
                return true;
            }
        } else if(obj instanceof EggLibrary && this instanceof EggLibrary) {
            if(((EggLibrary) obj).Uri.equals(((EggLibrary) this).Uri)) {
                return true;
            }
        } else if(obj instanceof MavenLibrary && this instanceof MavenLibrary) {
            if(((MavenLibrary) obj).Coordinates.equals(((MavenLibrary) this).Coordinates)) {
                return true;
            }
        } else if(obj instanceof PyPiLibrary && this instanceof PyPiLibrary) {
            if(((PyPiLibrary) obj).PackageName.equals(((PyPiLibrary) this).PackageName)) {
                return true;
            }
        } else if(obj instanceof CranLibrary && this instanceof CranLibrary) {
            if(((CranLibrary) obj).PackageName.equals(((CranLibrary) this).PackageName)) {
                return true;
            }
        }
        return (this == obj);
    }
}
