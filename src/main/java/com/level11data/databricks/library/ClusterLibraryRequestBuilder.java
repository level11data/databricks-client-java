package com.level11data.databricks.library;

import com.level11data.databricks.HttpException;
import com.level11data.databricks.LibrariesClient;
import com.level11data.databricks.entities.libraries.ClusterLibraryRequestDTO;
import com.level11data.databricks.entities.libraries.LibraryDTO;
import com.level11data.databricks.entities.libraries.MavenLibraryDTO;
import com.level11data.databricks.entities.libraries.PythonPyPiLibraryDTO;

import java.util.List;

public class ClusterLibraryRequestBuilder {
    private LibrariesClient _client;
    private String _clusterId;
    private List<LibraryDTO> _libraries;

    public ClusterLibraryRequestBuilder(LibrariesClient client, String clusterId) {
        _client = client;
        _clusterId = clusterId;
    }

    public ClusterLibraryRequestBuilder withJarLibrary(String uri) {
        LibraryDTO jarLibraryDTO = new LibraryDTO();
        jarLibraryDTO.Jar = uri;
        _libraries.add(jarLibraryDTO);
        return this;
    }

    public ClusterLibraryRequestBuilder withEggLibrary(String uri) {
        LibraryDTO eggLibraryDTO = new LibraryDTO();
        eggLibraryDTO.Egg = uri;
        _libraries.add(eggLibraryDTO);
        return this;
    }

    public ClusterLibraryRequestBuilder withPyPiLibrary(String pythonPackage, String repo) {
        PythonPyPiLibraryDTO pyPiLibrary = new PythonPyPiLibraryDTO();
        pyPiLibrary.Package = pythonPackage;
        pyPiLibrary.Repo = repo;

        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.PyPi = pyPiLibrary;
        _libraries.add(libraryDTO);
        return this;
    }

    public ClusterLibraryRequestBuilder withPyPiLibrary(String pythonPackage) {
        return withPyPiLibrary(pythonPackage, null);
    }

    public ClusterLibraryRequestBuilder withMavenLibrary(String coordinates,
                                                         String repo,
                                                         String[] exclusions) {
        MavenLibraryDTO mavenLibraryDTO = new MavenLibraryDTO();
        mavenLibraryDTO.Coordinates = coordinates;
        mavenLibraryDTO.Repo = repo;
        mavenLibraryDTO.Exclusions = exclusions;

        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Maven = mavenLibraryDTO;

        _libraries.add(libraryDTO);
        return this;
    }

    public ClusterLibraryRequestBuilder withMavenLibrary(String coordinates,
                                                         String repo) {
        return withMavenLibrary(coordinates, repo, null);
    }

    public ClusterLibraryRequestBuilder withMavenLibrary(String coordinates,
                                                         String[] exclusions) {
        return withMavenLibrary(coordinates, null, exclusions);
    }

    public ClusterLibraryRequestBuilder withMavenLibrary(String coordinates) {
        return withMavenLibrary(coordinates, null, null);
    }

    public void installLibraries() throws HttpException {
        ClusterLibraryRequestDTO request = new ClusterLibraryRequestDTO();
        request.ClusterId = _clusterId;
        request.Libraries = _libraries.toArray(new LibraryDTO[_libraries.size()]);
        _client.installLibraries(request);
    }

    public void uninstallLibraries() throws HttpException {
        ClusterLibraryRequestDTO request = new ClusterLibraryRequestDTO();
        request.ClusterId = _clusterId;
        request.Libraries = _libraries.toArray(new LibraryDTO[_libraries.size()]);
        _client.uninstallLibraries(request);
    }

}
