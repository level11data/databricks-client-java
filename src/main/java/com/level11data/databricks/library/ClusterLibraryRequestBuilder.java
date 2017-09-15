package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.entities.libraries.ClusterLibraryRequest;
import com.level11data.databricks.entities.libraries.Library;
import com.level11data.databricks.entities.libraries.MavenLibrary;
import com.level11data.databricks.entities.libraries.PythonPyPiLibrary;

import java.util.List;

public class ClusterLibraryRequestBuilder {
    private LibrariesClient _client;
    private String _clusterId;
    private List<Library> _libraries;

    public ClusterLibraryRequestBuilder(LibrariesClient client, String clusterId) {
        _client = client;
        _clusterId = clusterId;
    }

    public ClusterLibraryRequestBuilder withJarLibrary(String uri) {
        Library jarLibrary = new Library();
        jarLibrary.Jar = uri;
        _libraries.add(jarLibrary);
        return this;
    }

    public ClusterLibraryRequestBuilder withEggLibrary(String uri) {
        Library eggLibrary = new Library();
        eggLibrary.Egg = uri;
        _libraries.add(eggLibrary);
        return this;
    }

    public ClusterLibraryRequestBuilder withPyPiLibrary(String pythonPackage, String repo) {
        PythonPyPiLibrary pyPiLibrary = new PythonPyPiLibrary();
        pyPiLibrary.Package = pythonPackage;
        pyPiLibrary.Repo = repo;

        Library library = new Library();
        library.PyPi = pyPiLibrary;
        _libraries.add(library);
        return this;
    }

    public ClusterLibraryRequestBuilder withPyPiLibrary(String pythonPackage) {
        return withPyPiLibrary(pythonPackage, null);
    }

    public ClusterLibraryRequestBuilder withMavenLibrary(String coordinates,
                                                         String repo,
                                                         String[] exclusions) {
        MavenLibrary mavenLibrary = new MavenLibrary();
        mavenLibrary.Coordinates = coordinates;
        mavenLibrary.Repo = repo;
        mavenLibrary.Exclusions = exclusions;

        Library library = new Library();
        library.Maven = mavenLibrary;

        _libraries.add(library);
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
        ClusterLibraryRequest request = new ClusterLibraryRequest();
        request.ClusterId = _clusterId;
        request.Libraries = _libraries.toArray(new Library[_libraries.size()]);
        _client.installLibraries(request);
    }

    public void uninstallLibraries() throws HttpException {
        ClusterLibraryRequest request = new ClusterLibraryRequest();
        request.ClusterId = _clusterId;
        request.Libraries = _libraries.toArray(new Library[_libraries.size()]);
        _client.uninstallLibraries(request);
    }

}
