package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.SparkJarTaskDTO;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.job.AutomatedJarJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.JarLibrary;
import com.level11data.databricks.library.LibraryConfigException;
import org.quartz.Trigger;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class AutomatedJarJobBuilder extends AbstractAutomatedJobWithLibrariesBuilder {

    private final JobsClient _client;
    private final String _mainClassName;
    private List<String> _baseParameters;

    public AutomatedJarJobBuilder(JobsClient client,
                                  String mainClassName,
                                  JarLibrary jarLibrary,
                                  File libraryFile,
                                  List<String> parameters) {
        super(client, jarLibrary, libraryFile);
        _client = client;
        _mainClassName = mainClassName;

        if(parameters != null) {
            _baseParameters = parameters;
        } else {
            _baseParameters = new ArrayList<String>();
        }
    }

    public AutomatedJarJobBuilder(JobsClient client,
                                  String mainClassName,
                                  JarLibrary jarLibrary,
                                  File libraryFile) {
        this(client, mainClassName, jarLibrary, libraryFile, null);
    }

    public AutomatedJarJobBuilder(JobsClient client,
                                  String mainClassName,
                                  JarLibrary jarLibrary) {
        this(client, mainClassName, jarLibrary, null, null);
    }

    public AutomatedJarJobBuilder(JobsClient client,
                                  String mainClassName,
                                  JarLibrary jarLibrary,
                                  List<String> parameters) {
        this(client, mainClassName, jarLibrary, null, parameters);
    }

    @Override
    public AutomatedJarJobBuilder withName(String name) {
        return (AutomatedJarJobBuilder)super.withName(name);
    }

    @Override
    public AutomatedJarJobBuilder withEmailNotificationOnStart(String email) {
        return (AutomatedJarJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public AutomatedJarJobBuilder withEmailNotificationOnSuccess(String email) {
        return (AutomatedJarJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public AutomatedJarJobBuilder withEmailNotificationOnFailure(String email) {
        return (AutomatedJarJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public AutomatedJarJobBuilder withTimeout(int seconds) {
        return (AutomatedJarJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public AutomatedJarJobBuilder withMaxRetries(int retries) {
        return (AutomatedJarJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public AutomatedJarJobBuilder withMinRetryInterval(int milliseconds) {
        return (AutomatedJarJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public AutomatedJarJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (AutomatedJarJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public AutomatedJarJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (AutomatedJarJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public AutomatedJarJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (AutomatedJarJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public AutomatedJarJobBuilder withLibrary(Library library) {
        return (AutomatedJarJobBuilder)super.withLibrary(library);
    }

    @Override
    public AutomatedJarJobBuilder withJarLibrary(URI uri) {
        return (AutomatedJarJobBuilder)super.withJarLibrary(uri);
    }

    @Override
    public AutomatedJarJobBuilder withJarLibrary(URI uri, File libraryFile) {
        return (AutomatedJarJobBuilder)super.withJarLibrary(uri, libraryFile);
    }

    @Override
    public AutomatedJarJobBuilder withEggLibrary(URI uri) {
        return (AutomatedJarJobBuilder)super.withEggLibrary(uri);
    }

    @Override
    public AutomatedJarJobBuilder withEggLibrary(URI uri, File libraryFile) {
        return (AutomatedJarJobBuilder)super.withEggLibrary(uri, libraryFile);
    }

    @Override
    public AutomatedJarJobBuilder withMavenLibrary(String coordinates) {
        return (AutomatedJarJobBuilder)super.withMavenLibrary(coordinates);
    }

    @Override
    public AutomatedJarJobBuilder withMavenLibrary(String coordinates, String repo) {
        return (AutomatedJarJobBuilder)super.withMavenLibrary(coordinates, repo);
    }

    @Override
    public AutomatedJarJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        return (AutomatedJarJobBuilder)super.withMavenLibrary(coordinates, repo, exclusions);
    }

    @Override
    public AutomatedJarJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        return (AutomatedJarJobBuilder)super.withMavenLibrary(coordinates, exclusions);
    }

    @Override
    public AutomatedJarJobBuilder withPyPiLibrary(String packageName)  {
        return (AutomatedJarJobBuilder)super.withPyPiLibrary(packageName);
    }

    @Override
    public AutomatedJarJobBuilder withPyPiLibrary(String packageName, String repo) {
        return (AutomatedJarJobBuilder)super.withPyPiLibrary(packageName, repo);
    }

    @Override
    public AutomatedJarJobBuilder withCranLibrary(String packageName) {
        return (AutomatedJarJobBuilder)super.withCranLibrary(packageName);
    }

    @Override
    public AutomatedJarJobBuilder withCranLibrary(String packageName, String repo) {
        return (AutomatedJarJobBuilder)super.withCranLibrary(packageName, repo);
    }

    @Override
    public AutomatedJarJobBuilder withClusterSpec(ClusterSpec clusterSpec) {
        return (AutomatedJarJobBuilder)super.withClusterSpec(clusterSpec);
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public AutomatedJarJobBuilder withBaseParameter(String parameter) {
        _baseParameters.add(parameter);
        return this;
    }

    public AutomatedJarJob create() throws JobConfigException {
        try {
            //upload library files first
            uploadLibraryFiles();

            //no validation to perform
            JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
            jobSettingsDTO = super.applySettings(jobSettingsDTO);

            SparkJarTaskDTO jarTaskDTO = new SparkJarTaskDTO();
            jarTaskDTO.MainClassName = _mainClassName;

            if(_baseParameters != null) {
                jarTaskDTO.Parameters = _baseParameters.toArray(new String[_baseParameters.size()]);
            }

            jobSettingsDTO.SparkJarTask = jarTaskDTO;

            validate(jobSettingsDTO);

            return new AutomatedJarJob(_client, jobSettingsDTO, getLibraries());
        } catch(LibraryConfigException e) {
            throw new JobConfigException(e);
        }
    }
}
