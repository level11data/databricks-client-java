package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.MavenLibraryDTO;
import com.level11data.databricks.client.entities.libraries.PythonPyPiLibraryDTO;
import com.level11data.databricks.client.entities.libraries.RCranLibraryDTO;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.util.ResourceUtils;
import org.quartz.Trigger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public abstract class AbstractInteractiveJobBuilder extends AbstractJobBuilder implements JobBuilderWithLibraries{
    private final JobsClient _client;
    private ArrayList<Library> _libraries = new ArrayList<>();
    private ArrayList<LibraryDTO> _libraryDTOs = new ArrayList<>();
    private Map<URI, File> _libraryFileMap = new HashMap<>();

    public final InteractiveCluster Cluster;

    public AbstractInteractiveJobBuilder(InteractiveCluster cluster, JobsClient client) {
        super();
        Cluster = cluster;
        _client = client;
    }

    @Override
    public AbstractInteractiveJobBuilder withName(String name) {
        return (AbstractInteractiveJobBuilder)super.withName(name);
    }

    @Override
    public AbstractInteractiveJobBuilder withEmailNotificationOnStart(String email) {
        return (AbstractInteractiveJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public AbstractInteractiveJobBuilder withEmailNotificationOnSuccess(String email) {
        return (AbstractInteractiveJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public AbstractInteractiveJobBuilder withEmailNotificationOnFailure(String email) {
        return (AbstractInteractiveJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public AbstractInteractiveJobBuilder withTimeout(int seconds) {
        return (AbstractInteractiveJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public AbstractInteractiveJobBuilder withMaxRetries(int retries) {
        return (AbstractInteractiveJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public AbstractInteractiveJobBuilder withMinRetryInterval(int milliseconds) {
        return (AbstractInteractiveJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public AbstractInteractiveJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (AbstractInteractiveJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public AbstractInteractiveJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (AbstractInteractiveJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public AbstractInteractiveJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (AbstractInteractiveJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);
        jobSettingsDTO.ExistingClusterId = Cluster.getId();

        if(_libraryDTOs.size() > 0) {
            jobSettingsDTO.Libraries = _libraryDTOs.toArray(new LibraryDTO[_libraryDTOs.size()]);
        }
        return jobSettingsDTO;
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public AbstractInteractiveJobBuilder withLibrary(Library library) {
        _libraries.add(library);
        return this;
    }

    public AbstractInteractiveJobBuilder withJarLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = uri.toString();
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withJarLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withJarLibrary(uri);
    }

    public AbstractInteractiveJobBuilder withEggLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Egg = uri.toString();
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withEggLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withEggLibrary(uri);
    }

    public AbstractInteractiveJobBuilder withMavenLibrary(String coordinates) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withMavenLibrary(String coordinates, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withPyPiLibrary(String packageName)  {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        libraryDTO.PyPi = piPyDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withPyPiLibrary(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        piPyDTO.Repo = repo;
        libraryDTO.PyPi = piPyDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withCranLibrary(String packageName) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        libraryDTO.Cran = cranDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public AbstractInteractiveJobBuilder withCranLibrary(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        cranDTO.Repo = repo;
        libraryDTO.Cran = cranDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
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
}
