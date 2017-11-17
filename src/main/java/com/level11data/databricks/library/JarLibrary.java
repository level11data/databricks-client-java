package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryRequestDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.cluster.InteractiveCluster;

import java.net.URI;

public class JarLibrary extends Library {
    private final LibrariesClient _client;

    public final URI Uri;

    public JarLibrary(LibrariesClient client, URI uri) {
        //TODO validate only dbfs:// or s3:// or s3a:// etc
        _client = client;
        Uri = uri;
    }

    public void install(InteractiveCluster cluster) throws HttpException {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = Uri.toString();

        ClusterLibraryRequestDTO libraryRequestDTO = new ClusterLibraryRequestDTO();
        libraryRequestDTO.ClusterId = cluster.Id;
        libraryRequestDTO.Libraries = new LibraryDTO[1];
        libraryRequestDTO.Libraries[0] = libraryDTO;

        _client.installLibraries(libraryRequestDTO);
    }

    public void uninstall(InteractiveCluster cluster) throws HttpException {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = Uri.toString();

        ClusterLibraryRequestDTO libraryRequestDTO = new ClusterLibraryRequestDTO();
        libraryRequestDTO.ClusterId = cluster.Id;
        libraryRequestDTO.Libraries = new LibraryDTO[1];
        libraryRequestDTO.Libraries[0] = libraryDTO;

        _client.uninstallLibraries(libraryRequestDTO);
    }

}
