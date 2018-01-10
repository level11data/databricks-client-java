package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.MavenLibraryDTO;
import com.level11data.databricks.client.entities.libraries.PythonPyPiLibraryDTO;
import com.level11data.databricks.client.entities.libraries.RCranLibraryDTO;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class AutomatedJobWithLibrariesBuilder extends AutomatedJobBuilder {

    private JobsClient _client;
    private ArrayList<LibraryDTO> _libraries = new ArrayList<LibraryDTO>();
    private Map<URI, File> _libraryFileMap = new HashMap<URI, File>();

    public AutomatedJobWithLibrariesBuilder(JobsClient client) {
        super();
        _client = client;
    }

    protected AutomatedJobWithLibrariesBuilder withJarLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = uri.toString();
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withJarLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withJarLibrary(uri);
    }

    protected AutomatedJobWithLibrariesBuilder withEggLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Egg = uri.toString();
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withEggLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withEggLibrary(uri);
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        libraryDTO.Maven = mavenDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        libraryDTO.Maven = mavenDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName)  {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        libraryDTO.PyPi = piPyDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        piPyDTO.Repo = repo;
        libraryDTO.PyPi = piPyDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withCranLibrary(String packageName) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        libraryDTO.Cran = cranDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withCranLibrary(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        cranDTO.Repo = repo;
        libraryDTO.Cran = cranDTO;
        _libraries.add(libraryDTO);
        return this;
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        if(_libraries.size() > 0) {
            jobSettingsDTO.Libraries = _libraries.toArray(new LibraryDTO[_libraries.size()]);
        }
        return jobSettingsDTO;
    }

    private void addLibraryToUpload(URI destination, File libraryFile) {
        _libraryFileMap.put(destination, libraryFile);
    }

    protected void uploadLibraryFiles() throws HttpException, IOException, LibraryConfigException {
        for (URI uri : _libraryFileMap.keySet()) {
            FileUtils.uploadFile(_client.Session, _libraryFileMap.get(uri), uri);
        }
    }
}
