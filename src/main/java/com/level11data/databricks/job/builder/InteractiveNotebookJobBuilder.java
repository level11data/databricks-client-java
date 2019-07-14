package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.NotebookTaskDTO;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.workspace.Notebook;
import org.quartz.Trigger;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class InteractiveNotebookJobBuilder extends AbstractInteractiveJobBuilder {
    private final Notebook _notebook;
    private final JobsClient _client;
    private Map<String,String> _baseParameters;

    public InteractiveNotebookJobBuilder(JobsClient client,
                                         InteractiveCluster cluster,
                                         Notebook notebook) {
        this(client, cluster, notebook, new HashMap<>());
    }

    public InteractiveNotebookJobBuilder(JobsClient client,
                                         InteractiveCluster cluster,
                                         Notebook notebook,
                                         Map<String,String> baseParameters) {
        super(cluster, client);
        _client = client;
        _notebook = notebook;
        _baseParameters = baseParameters;
    }

    @Override
    public InteractiveNotebookJobBuilder withName(String name) {
        return (InteractiveNotebookJobBuilder)super.withName(name);
    }

    @Override
    public InteractiveNotebookJobBuilder withEmailNotificationOnStart(String email) {
        return (InteractiveNotebookJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public InteractiveNotebookJobBuilder withEmailNotificationOnSuccess(String email) {
        return (InteractiveNotebookJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public InteractiveNotebookJobBuilder withEmailNotificationOnFailure(String email) {
        return (InteractiveNotebookJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public InteractiveNotebookJobBuilder withTimeout(int seconds) {
        return (InteractiveNotebookJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public InteractiveNotebookJobBuilder withMaxRetries(int retries) {
        return (InteractiveNotebookJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public InteractiveNotebookJobBuilder withMinRetryInterval(int milliseconds) {
        return (InteractiveNotebookJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public InteractiveNotebookJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (InteractiveNotebookJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public InteractiveNotebookJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (InteractiveNotebookJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public InteractiveNotebookJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (InteractiveNotebookJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public InteractiveNotebookJobBuilder withJarLibrary(URI uri) {
        return (InteractiveNotebookJobBuilder)super.withJarLibrary(uri);
    }

    @Override
    public InteractiveNotebookJobBuilder withJarLibrary(URI uri, File libraryFile) {
        return (InteractiveNotebookJobBuilder)super.withJarLibrary(uri, libraryFile);
    }

    @Override
    public InteractiveNotebookJobBuilder withEggLibrary(URI uri) {
        return (InteractiveNotebookJobBuilder)super.withEggLibrary(uri);
    }

    @Override
    public InteractiveNotebookJobBuilder withEggLibrary(URI uri, File libraryFile) {
        return (InteractiveNotebookJobBuilder)super.withEggLibrary(uri, libraryFile);
    }

    @Override
    public InteractiveNotebookJobBuilder withMavenLibrary(String coordinates) {
        return (InteractiveNotebookJobBuilder)super.withMavenLibrary(coordinates);
    }

    @Override
    public InteractiveNotebookJobBuilder withMavenLibrary(String coordinates, String repo) {
        return (InteractiveNotebookJobBuilder)super.withMavenLibrary(coordinates, repo);
    }

    @Override
    public InteractiveNotebookJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        return (InteractiveNotebookJobBuilder)super.withMavenLibrary(coordinates, repo, exclusions);
    }

    @Override
    public InteractiveNotebookJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        return (InteractiveNotebookJobBuilder)super.withMavenLibrary(coordinates, exclusions);
    }

    @Override
    public InteractiveNotebookJobBuilder withPyPiLibrary(String packageName)  {
        return (InteractiveNotebookJobBuilder)super.withPyPiLibrary(packageName);
    }

    @Override
    public InteractiveNotebookJobBuilder withPyPiLibrary(String packageName, String repo) {
        return (InteractiveNotebookJobBuilder)super.withPyPiLibrary(packageName, repo);
    }

    @Override
    public InteractiveNotebookJobBuilder withCranLibrary(String packageName) {
        return (InteractiveNotebookJobBuilder)super.withCranLibrary(packageName);
    }

    @Override
    public InteractiveNotebookJobBuilder withCranLibrary(String packageName, String repo) {
        return (InteractiveNotebookJobBuilder)super.withCranLibrary(packageName, repo);
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public InteractiveNotebookJob create() throws JobConfigException {
        try {
            JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
            jobSettingsDTO = super.applySettings(jobSettingsDTO);

            NotebookTaskDTO notebookTaskDTO = new NotebookTaskDTO();
            notebookTaskDTO.NotebookPath = _notebook.getWorkspacePath();
            notebookTaskDTO.BaseParameters = _baseParameters;
            jobSettingsDTO.NotebookTask = notebookTaskDTO;

            validate(jobSettingsDTO);

            //upload any library files
            uploadLibraryFiles();

            return new InteractiveNotebookJob(_client, this.Cluster, jobSettingsDTO, _notebook, getLibraries());
        } catch(LibraryConfigException e) {
            throw new JobConfigException(e);
        }
    }

    public InteractiveNotebookJobBuilder withBaseParameter(String key, String value) {
        _baseParameters.put(key, value);
        return this;
    }
}
