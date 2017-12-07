package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryRequestDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.cluster.InteractiveCluster;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class JarLibrary extends Library {
    private final LibrariesClient _client;

    public JarLibrary(LibrariesClient client, URI uri) throws LibraryConfigException {
        super(uri);
        _client = client;
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

    public void upload(File file) throws HttpException, IOException, LibraryConfigException{
        if(Uri.getScheme().equals("dbfs")) {
            _client.Session.putDbfsFile(file, Uri.toString());
        } else {
            throw new LibraryConfigException(Uri.getScheme() + " is not a supported scheme for upload");
        }

    }



}
