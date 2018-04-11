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
import com.level11data.databricks.library.ILibrary;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.util.ResourceUtils;
import org.quartz.Trigger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public abstract class InteractiveJobBuilder extends JobBuilder {
    private final JobsClient _client;
    private ArrayList<ILibrary> _libraries = new ArrayList<>();
    private ArrayList<LibraryDTO> _libraryDTOs = new ArrayList<>();
    private Map<URI, File> _libraryFileMap = new HashMap<>();

    public final InteractiveCluster Cluster;

    public InteractiveJobBuilder(InteractiveCluster cluster, JobsClient client) {
        super();
        Cluster = cluster;
        _client = client;
    }

    @Override
    public InteractiveJobBuilder withName(String name) {
        return (InteractiveJobBuilder)super.withName(name);
    }

    @Override
    public InteractiveJobBuilder withEmailNotificationOnStart(String email) {
        return (InteractiveJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public InteractiveJobBuilder withEmailNotificationOnSuccess(String email) {
        return (InteractiveJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public InteractiveJobBuilder withEmailNotificationOnFailure(String email) {
        return (InteractiveJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public InteractiveJobBuilder withTimeout(int seconds) {
        return (InteractiveJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public InteractiveJobBuilder withMaxRetries(int retries) {
        return (InteractiveJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public InteractiveJobBuilder withMinRetryInterval(int milliseconds) {
        return (InteractiveJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public InteractiveJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (InteractiveJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public InteractiveJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (InteractiveJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public InteractiveJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (InteractiveJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);
        jobSettingsDTO.ExistingClusterId = Cluster.Id;

        if(_libraryDTOs.size() > 0) {
            jobSettingsDTO.Libraries = _libraryDTOs.toArray(new LibraryDTO[_libraryDTOs.size()]);
        }
        return jobSettingsDTO;
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public InteractiveJobBuilder withLibrary(ILibrary library) {
        _libraries.add(library);
        return this;
    }

    public InteractiveJobBuilder withJarLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = uri.toString();
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withJarLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withJarLibrary(uri);
    }

    public InteractiveJobBuilder withEggLibrary(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Egg = uri.toString();
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withEggLibrary(URI uri, File libraryFile) {
        addLibraryToUpload(uri, libraryFile);
        return this.withEggLibrary(uri);
    }

    public InteractiveJobBuilder withMavenLibrary(String coordinates) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withMavenLibrary(String coordinates, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Repo = repo;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;
        mavenDTO.Exclusions = exclusions;
        libraryDTO.Maven = mavenDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withPyPiLibrary(String packageName)  {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        libraryDTO.PyPi = piPyDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withPyPiLibrary(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;
        piPyDTO.Repo = repo;
        libraryDTO.PyPi = piPyDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withCranLibrary(String packageName) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        libraryDTO.Cran = cranDTO;
        _libraryDTOs.add(libraryDTO);
        return this;
    }

    public InteractiveJobBuilder withCranLibrary(String packageName, String repo) {
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
            } catch (HttpException e) {
                throw new LibraryConfigException(e);
            } catch (ResourceConfigException e) {
                throw new LibraryConfigException(e);
            } catch (IOException e) {
                throw new LibraryConfigException(e);
            }
        }
    }

    protected List<ILibrary> getLibraries() {
        return _libraries;
    }
}
