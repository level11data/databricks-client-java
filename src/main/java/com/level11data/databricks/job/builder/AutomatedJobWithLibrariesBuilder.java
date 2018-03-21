package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.libraries.*;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.library.PrivateLibrary;
import com.level11data.databricks.library.util.LibraryHelper;
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
            _libraryDTOs.add(LibraryHelper.createLibraryDTO(library)); //needed to create job via API
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
        _libraryDTOs.add(LibraryHelper.createJarLibraryDTO(uri));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withJarLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withJarLibrary(uri);
    }

    protected AutomatedJobWithLibrariesBuilder withEggLibrary(URI uri) {
        _libraryDTOs.add(LibraryHelper.createEggLibraryDTO(uri));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withEggLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withEggLibrary(uri);
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates, repo));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates, repo, exclusions));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates, exclusions));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName)  {
        _libraryDTOs.add(LibraryHelper.createPyPiLibraryDTO(packageName));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName, String repo) {
        _libraryDTOs.add(LibraryHelper.createPyPiLibraryDTO(packageName, repo));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withCranLibrary(String packageName) {
        _libraryDTOs.add(LibraryHelper.createCranLibraryDTO(packageName));
        return this;
    }

    protected AutomatedJobWithLibrariesBuilder withCranLibrary(String packageName, String repo) {
        _libraryDTOs.add(LibraryHelper.createCranLibraryDTO(packageName, repo));
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
