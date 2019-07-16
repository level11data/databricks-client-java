package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.PythonTaskDTO;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.job.InteractivePythonJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.job.PythonScript;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.util.ResourceConfigException;
import org.quartz.Trigger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class InteractivePythonJobBuilder extends AbstractInteractiveJobBuilder {
    private final JobsClient _client;
    private final PythonScript _pythonScript;
    private final File _pythonFile;
    private List<String> _baseParameters;

    public InteractivePythonJobBuilder(JobsClient client,
                                       InteractiveCluster cluster,
                                       PythonScript pythonScript,
                                       File pythonFile,
                                       List<String> baseParameters) {
        super(cluster, client);
        _client = client;
        _pythonScript = pythonScript;
        _pythonFile = pythonFile;          //could be null

        if(baseParameters != null) {
            _baseParameters = baseParameters;
        } else {
            _baseParameters = new ArrayList<String>();
        }
    }

    public InteractivePythonJobBuilder(JobsClient client,
                                       InteractiveCluster cluster,
                                       PythonScript pythonScript,
                                       File pythonFile) {
        this(client, cluster, pythonScript, pythonFile, null);
    }

    public InteractivePythonJobBuilder(JobsClient client,
                                       InteractiveCluster cluster,
                                       PythonScript pythonScript) {
        this(client, cluster, pythonScript, null, null);
    }

    public InteractivePythonJobBuilder(JobsClient client,
                                       InteractiveCluster cluster,
                                       PythonScript pythonScript,
                                       List<String> baseParameters) {
        this(client, cluster, pythonScript, null, baseParameters);
    }

    @Override
    public InteractivePythonJobBuilder withName(String name) {
        return (InteractivePythonJobBuilder)super.withName(name);
    }

    @Override
    public InteractivePythonJobBuilder withEmailNotificationOnStart(String email) {
        return (InteractivePythonJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public InteractivePythonJobBuilder withEmailNotificationOnSuccess(String email) {
        return (InteractivePythonJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public InteractivePythonJobBuilder withEmailNotificationOnFailure(String email) {
        return (InteractivePythonJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public InteractivePythonJobBuilder withTimeout(int seconds) {
        return (InteractivePythonJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public InteractivePythonJobBuilder withMaxRetries(int retries) {
        return (InteractivePythonJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public InteractivePythonJobBuilder withMinRetryInterval(int milliseconds) {
        return (InteractivePythonJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public InteractivePythonJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (InteractivePythonJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public InteractivePythonJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (InteractivePythonJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public InteractivePythonJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (InteractivePythonJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public InteractivePythonJobBuilder withLibrary(Library library) {
        return (InteractivePythonJobBuilder)super.withLibrary(library);
    }

    @Override
    public InteractivePythonJobBuilder withJarLibrary(URI uri) {
        return (InteractivePythonJobBuilder)super.withJarLibrary(uri);
    }

    @Override
    public InteractivePythonJobBuilder withJarLibrary(URI uri, File libraryFile) {
        return (InteractivePythonJobBuilder)super.withJarLibrary(uri, libraryFile);
    }

    @Override
    public InteractivePythonJobBuilder withEggLibrary(URI uri) {
        return (InteractivePythonJobBuilder)super.withEggLibrary(uri);
    }

    @Override
    public InteractivePythonJobBuilder withEggLibrary(URI uri, File libraryFile) {
        return (InteractivePythonJobBuilder)super.withEggLibrary(uri, libraryFile);
    }

    @Override
    public InteractivePythonJobBuilder withMavenLibrary(String coordinates) {
        return (InteractivePythonJobBuilder)super.withMavenLibrary(coordinates);
    }

    @Override
    public InteractivePythonJobBuilder withMavenLibrary(String coordinates, String repo) {
        return (InteractivePythonJobBuilder)super.withMavenLibrary(coordinates, repo);
    }

    @Override
    public InteractivePythonJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        return (InteractivePythonJobBuilder)super.withMavenLibrary(coordinates, repo, exclusions);
    }

    @Override
    public InteractivePythonJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        return (InteractivePythonJobBuilder)super.withMavenLibrary(coordinates, exclusions);
    }

    @Override
    public InteractivePythonJobBuilder withPyPiLibrary(String packageName)  {
        return (InteractivePythonJobBuilder)super.withPyPiLibrary(packageName);
    }

    @Override
    public InteractivePythonJobBuilder withPyPiLibrary(String packageName, String repo) {
        return (InteractivePythonJobBuilder)super.withPyPiLibrary(packageName, repo);
    }

    @Override
    public InteractivePythonJobBuilder withCranLibrary(String packageName) {
        return (InteractivePythonJobBuilder)super.withCranLibrary(packageName);
    }

    @Override
    public InteractivePythonJobBuilder withCranLibrary(String packageName, String repo) {
        return (InteractivePythonJobBuilder)super.withCranLibrary(packageName, repo);
    }

    public InteractivePythonJobBuilder withBaseParameter(String parameter) {
        _baseParameters.add(parameter);
        return this;
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public InteractivePythonJob create() throws JobConfigException {
        try {
            //upload file first, if supplied
            if(_pythonFile != null) {
                _pythonScript.upload(_pythonFile);
            }

            //upload library files
            uploadLibraryFiles();

            //create DTO
            JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
            jobSettingsDTO = super.applySettings(jobSettingsDTO);

            PythonTaskDTO pythonTaskDTO = new PythonTaskDTO();
            pythonTaskDTO.PythonFile = _pythonScript.Uri.toString();

            if(_baseParameters.size() > 0) {
                pythonTaskDTO.Parameters = _baseParameters.toArray(new String[_baseParameters.size()]);
            }
            jobSettingsDTO.SparkPythonTask = pythonTaskDTO;

            validate(jobSettingsDTO);

            return new InteractivePythonJob(_client, this.Cluster, _pythonScript, jobSettingsDTO, getLibraries());
        } catch (HttpException e) {
            throw new JobConfigException(e);
        } catch (LibraryConfigException e) {
            throw new JobConfigException(e);
        } catch (ResourceConfigException e) {
            throw new JobConfigException(e);
        } catch (IOException e) {
            throw new JobConfigException(e);
        }
    }


}
