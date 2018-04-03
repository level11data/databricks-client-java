package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.SparkJarTaskDTO;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.job.InteractiveJarJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.LibraryConfigException;
import org.quartz.Trigger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class InteractiveJarJobBuilder extends InteractiveJobBuilder {
    private final JobsClient _client;
    private final String _mainClassName;
    private final List<String> _baseParameters;

    public InteractiveJarJobBuilder(JobsClient client,
                                    InteractiveCluster cluster,
                                    URI jarLibraryLocation,  //TODO replace this with JarLibrary
                                    String mainClassName) {
        this(client, cluster, jarLibraryLocation, null, mainClassName,new ArrayList<String>());
    }

    public InteractiveJarJobBuilder(JobsClient client,
                                    InteractiveCluster cluster,
                                    URI jarLibraryLocation,
                                    String mainClassName,
                                    List<String> baseParameters) {
        this(client, cluster, jarLibraryLocation, null, mainClassName, baseParameters);
    }

    public InteractiveJarJobBuilder(JobsClient client,
                                    InteractiveCluster cluster,
                                    URI jarLibraryLocation,
                                    File jarLibraryFile,
                                    String mainClassName) {
        this(client, cluster, jarLibraryLocation, jarLibraryFile, mainClassName, new ArrayList<String>());
    }

    public InteractiveJarJobBuilder(JobsClient client,
                                    InteractiveCluster cluster,
                                    URI jarLibraryLocation,
                                    File jarLibraryFile,
                                    String mainClassName,
                                    List<String> baseParameters) {
        super(cluster, client);
        _client = client;
        _mainClassName = mainClassName;
        _baseParameters = baseParameters;

        if(jarLibraryFile != null) {
            withJarLibrary(jarLibraryLocation, jarLibraryFile);
        } else {
            withJarLibrary(jarLibraryLocation);
        }
    }

    @Override
    public InteractiveJarJobBuilder withName(String name) {
        return (InteractiveJarJobBuilder)super.withName(name);
    }

    @Override
    public InteractiveJarJobBuilder withEmailNotificationOnStart(String email) {
        return (InteractiveJarJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public InteractiveJarJobBuilder withEmailNotificationOnSuccess(String email) {
        return (InteractiveJarJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public InteractiveJarJobBuilder withEmailNotificationOnFailure(String email) {
        return (InteractiveJarJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public InteractiveJarJobBuilder withTimeout(int seconds) {
        return (InteractiveJarJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public InteractiveJarJobBuilder withMaxRetries(int retries) {
        return (InteractiveJarJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public InteractiveJarJobBuilder withMinRetryInterval(int milliseconds) {
        return (InteractiveJarJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public InteractiveJarJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (InteractiveJarJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public InteractiveJarJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (InteractiveJarJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public InteractiveJarJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (InteractiveJarJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public InteractiveJarJobBuilder withJarLibrary(URI uri) {
        return (InteractiveJarJobBuilder)super.withJarLibrary(uri);
    }

    @Override
    public InteractiveJarJobBuilder withJarLibrary(URI uri, File libraryFile) {
        return (InteractiveJarJobBuilder)super.withJarLibrary(uri, libraryFile);
    }

    @Override
    public InteractiveJarJobBuilder withEggLibrary(URI uri) {
        return (InteractiveJarJobBuilder)super.withEggLibrary(uri);
    }

    @Override
    public InteractiveJarJobBuilder withEggLibrary(URI uri, File libraryFile) {
        return (InteractiveJarJobBuilder)super.withEggLibrary(uri, libraryFile);
    }

    @Override
    public InteractiveJarJobBuilder withMavenLibrary(String coordinates) {
        return (InteractiveJarJobBuilder)super.withMavenLibrary(coordinates);
    }

    @Override
    public InteractiveJarJobBuilder withMavenLibrary(String coordinates, String repo) {
        return (InteractiveJarJobBuilder)super.withMavenLibrary(coordinates, repo);
    }

    @Override
    public InteractiveJarJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        return (InteractiveJarJobBuilder)super.withMavenLibrary(coordinates, repo, exclusions);
    }

    @Override
    public InteractiveJarJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        return (InteractiveJarJobBuilder)super.withMavenLibrary(coordinates, exclusions);
    }

    @Override
    public InteractiveJarJobBuilder withPyPiLibrary(String packageName)  {
        return (InteractiveJarJobBuilder)super.withPyPiLibrary(packageName);
    }

    @Override
    public InteractiveJarJobBuilder withPyPiLibrary(String packageName, String repo) {
        return (InteractiveJarJobBuilder)super.withPyPiLibrary(packageName, repo);
    }

    @Override
    public InteractiveJarJobBuilder withCranLibrary(String packageName) {
        return (InteractiveJarJobBuilder)super.withCranLibrary(packageName);
    }

    @Override
    public InteractiveJarJobBuilder withCranLibrary(String packageName, String repo) {
        return (InteractiveJarJobBuilder)super.withCranLibrary(packageName, repo);
    }


    public InteractiveJarJob create() throws HttpException, IOException, LibraryConfigException, URISyntaxException, JobConfigException {
        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        SparkJarTaskDTO jarTaskDTO = new SparkJarTaskDTO();
        jarTaskDTO.MainClassName = _mainClassName;

        if(_baseParameters.size() > 0) {
            jarTaskDTO.Parameters = _baseParameters.toArray(new String[_baseParameters.size()]);
        }
        jobSettingsDTO.SparkJarTask = jarTaskDTO;

        //upload library files
        uploadLibraryFiles();

        return new InteractiveJarJob(_client, this.Cluster, jobSettingsDTO);
    }


}
