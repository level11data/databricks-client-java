package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.MavenLibraryDTO;
import com.level11data.databricks.client.entities.libraries.PythonPyPiLibraryDTO;
import com.level11data.databricks.client.entities.libraries.RCranLibraryDTO;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.library.PrivateLibrary;
import com.level11data.databricks.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AutomatedJobWithLibrariesBuilder extends AutomatedJobBuilder {

    private JobsClient _client;
    private ArrayList<LibraryDTO> _libraryDTOs = new ArrayList<LibraryDTO>();
    private ArrayList<Library> _libraries = new ArrayList<Library>();
    private Map<URI, File> _libraryFileMap = new HashMap<URI, File>();

    public AutomatedJobWithLibrariesBuilder(JobsClient client) {
        this(client, null, null);
    }

    public AutomatedJobWithLibrariesBuilder(JobsClient client, PrivateLibrary library) {
        this(client, library, null);
    }

    public AutomatedJobWithLibrariesBuilder(JobsClient client, PrivateLibrary library, File libraryFile) {
        super();
        _client = client;

        if(library != null) {
            _libraries.add(library);  //will maintain library object reference to Job
            _libraryDTOs.add(library.createLibraryDTO()); //needed to create job via API
        }

        if(libraryFile != null) {
            addLibraryToUpload(library.Uri, libraryFile);
        }
    }

    protected AutomatedJobWithLibrariesBuilder withLibrary(Library library) {
        _libraries.add(library);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withJarLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = uri.toString();
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withJarLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withJarLibrary(uri);
    }

    protected AutomatedJobWithLibrariesBuilder withEggLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Egg = uri.toString();
        _libraryDTOs.add(libraryDTO);
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
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName)  {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        libraryDTO.PyPi = piPyDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        piPyDTO.Repo = repo;
        libraryDTO.PyPi = piPyDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withCranLibrary(String packageName) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        libraryDTO.Cran = cranDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withCranLibrary(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        cranDTO.Repo = repo;
        libraryDTO.Cran = cranDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        if(_libraryDTOs.size() > 0) {
            jobSettingsDTO.Libraries = _libraryDTOs.toArray(new LibraryDTO[_libraryDTOs.size()]);
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

    protected List<Library> getLibraries() {
        return _libraries;
    }
}
