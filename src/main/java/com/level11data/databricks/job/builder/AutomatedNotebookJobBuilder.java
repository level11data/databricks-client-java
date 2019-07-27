package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.NotebookTaskDTO;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.workspace.Notebook;
import com.level11data.databricks.workspace.WorkspaceConfigException;
import org.quartz.Trigger;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class AutomatedNotebookJobBuilder extends AbstractAutomatedJobWithLibrariesBuilder {
    private final Notebook _notebook;
    private final JobsClient _client;
    private Map<String,String> _baseParameters;


    public AutomatedNotebookJobBuilder(JobsClient client, Notebook notebook) {
        this(client, notebook, new HashMap<>());
    }

    public AutomatedNotebookJobBuilder(JobsClient client, Notebook notebook, Map<String,String> baseParameters) {
        super(client);
        _client = client;
        _notebook = notebook;
        _baseParameters = baseParameters;
    }

    @Override
    public AutomatedNotebookJobBuilder withName(String name) {
        return (AutomatedNotebookJobBuilder)super.withName(name);
    }

    @Override
    public AutomatedNotebookJobBuilder withEmailNotificationOnStart(String email) {
        return (AutomatedNotebookJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public AutomatedNotebookJobBuilder withEmailNotificationOnSuccess(String email) {
        return (AutomatedNotebookJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public AutomatedNotebookJobBuilder withEmailNotificationOnFailure(String email) {
        return (AutomatedNotebookJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public AutomatedNotebookJobBuilder withTimeout(int seconds) {
        return (AutomatedNotebookJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public AutomatedNotebookJobBuilder withMaxRetries(int retries) {
        return (AutomatedNotebookJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public AutomatedNotebookJobBuilder withMinRetryInterval(int milliseconds) {
        return (AutomatedNotebookJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public AutomatedNotebookJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (AutomatedNotebookJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public AutomatedNotebookJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (AutomatedNotebookJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public AutomatedNotebookJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (AutomatedNotebookJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public AutomatedNotebookJobBuilder withLibrary(Library library) {
        return (AutomatedNotebookJobBuilder)super.withLibrary(library);
    }

    @Override
    public AutomatedNotebookJobBuilder withJarLibrary(URI uri) {
        return (AutomatedNotebookJobBuilder)super.withJarLibrary(uri);
    }

    @Override
    public AutomatedNotebookJobBuilder withJarLibrary(URI uri, File libraryFile) {
        return (AutomatedNotebookJobBuilder)super.withJarLibrary(uri, libraryFile);
    }

    @Override
    public AutomatedNotebookJobBuilder withEggLibrary(URI uri) {
        return (AutomatedNotebookJobBuilder)super.withEggLibrary(uri);
    }

    @Override
    public AutomatedNotebookJobBuilder withEggLibrary(URI uri, File libraryFile) {
        return (AutomatedNotebookJobBuilder)super.withEggLibrary(uri, libraryFile);
    }

    @Override
    public AutomatedNotebookJobBuilder withMavenLibrary(String coordinates) {
        return (AutomatedNotebookJobBuilder)super.withMavenLibrary(coordinates);
    }

    @Override
    public AutomatedNotebookJobBuilder withMavenLibrary(String coordinates, String repo) {
        return (AutomatedNotebookJobBuilder)super.withMavenLibrary(coordinates, repo);
    }

    @Override
    public AutomatedNotebookJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        return (AutomatedNotebookJobBuilder)super.withMavenLibrary(coordinates, repo, exclusions);
    }

    @Override
    public AutomatedNotebookJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        return (AutomatedNotebookJobBuilder)super.withMavenLibrary(coordinates, exclusions);
    }

    @Override
    public AutomatedNotebookJobBuilder withPyPiLibrary(String packageName)  {
        return (AutomatedNotebookJobBuilder)super.withPyPiLibrary(packageName);
    }

    @Override
    public AutomatedNotebookJobBuilder withPyPiLibrary(String packageName, String repo) {
        return (AutomatedNotebookJobBuilder)super.withPyPiLibrary(packageName, repo);
    }

    @Override
    public AutomatedNotebookJobBuilder withCranLibrary(String packageName) {
        return (AutomatedNotebookJobBuilder)super.withCranLibrary(packageName);
    }

    @Override
    public AutomatedNotebookJobBuilder withCranLibrary(String packageName, String repo) {
        return (AutomatedNotebookJobBuilder)super.withCranLibrary(packageName, repo);
    }

    @Override
    public AutomatedNotebookJobBuilder withClusterSpec(ClusterSpec clusterSpec) {
        return (AutomatedNotebookJobBuilder)super.withClusterSpec(clusterSpec);
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public AutomatedNotebookJob create() throws JobConfigException {
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

            //create InteractiveNotebookJob from jobSettingsDTO and jobId
            return new AutomatedNotebookJob(_client, jobSettingsDTO, _notebook);
        } catch(LibraryConfigException e) {
            throw new JobConfigException(e);
        }
    }

    public AutomatedNotebookJobBuilder withBaseParameter(String key, String value) {
        _baseParameters.put(key, value);
        return this;
    }
}
