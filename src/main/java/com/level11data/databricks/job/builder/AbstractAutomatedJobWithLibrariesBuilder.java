package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.libraries.*;
import com.level11data.databricks.dbfs.DbfsException;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.library.AbstractPrivateLibrary;
import com.level11data.databricks.library.util.LibraryHelper;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractAutomatedJobWithLibrariesBuilder extends AbstractAutomatedJobBuilder implements JobBuilderWithLibraries {

    private JobsClient _client;
    private ArrayList<LibraryDTO> _libraryDTOs = new ArrayList<>();
    private ArrayList<Library> _libraries = new ArrayList<>();
    private Map<URI, File> _libraryFileMap = new HashMap<>();

    public AbstractAutomatedJobWithLibrariesBuilder(JobsClient client) {
        this(client, null, null);
    }

    public AbstractAutomatedJobWithLibrariesBuilder(JobsClient client, AbstractPrivateLibrary library) {
        this(client, library, null);
    }

    public AbstractAutomatedJobWithLibrariesBuilder(JobsClient client, AbstractPrivateLibrary library, File libraryFile) {
        super();
        _client = client;

        if(library != null) {
            _libraries.add(library);  //will maintain library object reference to AbstractJob
            _libraryDTOs.add(LibraryHelper.createLibraryDTO(library)); //needed to create job via API
        }

        if(libraryFile != null) {
            addLibraryToUpload(library.Uri, libraryFile);
        }
    }

    public AbstractAutomatedJobWithLibrariesBuilder withLibrary(Library library) {
        _libraries.add(library);
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withJarLibrary(URI uri) {
        _libraryDTOs.add(LibraryHelper.createJarLibraryDTO(uri));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withJarLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withJarLibrary(uri);
    }

    public AbstractAutomatedJobWithLibrariesBuilder withEggLibrary(URI uri) {
        _libraryDTOs.add(LibraryHelper.createEggLibraryDTO(uri));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withEggLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withEggLibrary(uri);
    }

    public AbstractAutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates, repo));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates, repo, exclusions));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        _libraryDTOs.add(LibraryHelper.createMavenLibraryDTO(coordinates, exclusions));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName)  {
        _libraryDTOs.add(LibraryHelper.createPyPiLibraryDTO(packageName));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withPyPiLibrary(String packageName, String repo) {
        _libraryDTOs.add(LibraryHelper.createPyPiLibraryDTO(packageName, repo));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withCranLibrary(String packageName) {
        _libraryDTOs.add(LibraryHelper.createCranLibraryDTO(packageName));
        return this;
    }

    public AbstractAutomatedJobWithLibrariesBuilder withCranLibrary(String packageName, String repo) {
        _libraryDTOs.add(LibraryHelper.createCranLibraryDTO(packageName, repo));
        return this;
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        if(_libraryDTOs.size() > 0) {
            jobSettingsDTO.Libraries = _libraryDTOs.toArray(new LibraryDTO[_libraryDTOs.size()]);
        }
        return jobSettingsDTO;
    }

    private void addLibraryToUpload(URI destination, File libraryFile) {
        _libraryFileMap.put(destination, libraryFile);
    }

    protected void uploadLibraryFiles() throws LibraryConfigException {
        for (URI uri : _libraryFileMap.keySet()) {
            try {
                ResourceUtils.uploadFile(_client.Session, _libraryFileMap.get(uri), uri);
            } catch (ResourceConfigException e) {
                throw new LibraryConfigException(e);
            }
        }
    }

    protected List<Library> getLibraries() {
        return _libraries;
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }
}
