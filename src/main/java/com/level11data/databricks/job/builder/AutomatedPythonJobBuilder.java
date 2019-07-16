package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.PythonTaskDTO;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.job.AutomatedPythonJob;
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

public class AutomatedPythonJobBuilder extends AbstractAutomatedJobWithLibrariesBuilder {

    private final JobsClient _client;
    private final PythonScript _pythonScript;
    private final File _pythonFile;
    private  List<String> _baseParameters;

    //everything
    public AutomatedPythonJobBuilder(JobsClient client,
                                     PythonScript pythonScript,
                                     File pythonFile,
                                     List<String> parameters) {
        super(client);
        _client = client;
        _pythonScript = pythonScript;
        _pythonFile = pythonFile;      //could be null

        if(parameters != null) {
            _baseParameters = parameters;
        } else {
            _baseParameters = new ArrayList<String>();
        }
    }

    //no parameters
    public AutomatedPythonJobBuilder(JobsClient client,
                                     PythonScript pythonScript,
                                     File pythonFile) {
        this(client, pythonScript, pythonFile, null);
    }

    //no parameters, no file
    public AutomatedPythonJobBuilder(JobsClient client,
                                     PythonScript pythonScript) {
        this(client, pythonScript, null, null);
    }

    //no file
    public AutomatedPythonJobBuilder(JobsClient client,
                                     PythonScript pythonScript,
                                     List<String> parameters) {
        this(client, pythonScript, null, parameters);
    }

    @Override
    public AutomatedPythonJobBuilder withName(String name) {
        return (AutomatedPythonJobBuilder)super.withName(name);
    }

    @Override
    public AutomatedPythonJobBuilder withEmailNotificationOnStart(String email) {
        return (AutomatedPythonJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public AutomatedPythonJobBuilder withEmailNotificationOnSuccess(String email) {
        return (AutomatedPythonJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public AutomatedPythonJobBuilder withEmailNotificationOnFailure(String email) {
        return (AutomatedPythonJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public AutomatedPythonJobBuilder withTimeout(int seconds) {
        return (AutomatedPythonJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public AutomatedPythonJobBuilder withMaxRetries(int retries) {
        return (AutomatedPythonJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public AutomatedPythonJobBuilder withMinRetryInterval(int milliseconds) {
        return (AutomatedPythonJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public AutomatedPythonJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (AutomatedPythonJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public AutomatedPythonJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (AutomatedPythonJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public AutomatedPythonJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (AutomatedPythonJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public AutomatedPythonJobBuilder withLibrary(Library library) {
        return (AutomatedPythonJobBuilder)super.withLibrary(library);
    }

    @Override
    public AutomatedPythonJobBuilder withJarLibrary(URI uri) {
        return (AutomatedPythonJobBuilder)super.withJarLibrary(uri);
    }

    @Override
    public AutomatedPythonJobBuilder withJarLibrary(URI uri, File libraryFile) {
        return (AutomatedPythonJobBuilder)super.withJarLibrary(uri, libraryFile);
    }

    @Override
    public AutomatedPythonJobBuilder withEggLibrary(URI uri) {
        return (AutomatedPythonJobBuilder)super.withEggLibrary(uri);
    }

    @Override
    public AutomatedPythonJobBuilder withEggLibrary(URI uri, File libraryFile) {
        return (AutomatedPythonJobBuilder)super.withEggLibrary(uri, libraryFile);
    }

    @Override
    public AutomatedPythonJobBuilder withMavenLibrary(String coordinates) {
        return (AutomatedPythonJobBuilder)super.withMavenLibrary(coordinates);
    }

    @Override
    public AutomatedPythonJobBuilder withMavenLibrary(String coordinates, String repo) {
        return (AutomatedPythonJobBuilder)super.withMavenLibrary(coordinates, repo);
    }

    @Override
    public AutomatedPythonJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) {
        return (AutomatedPythonJobBuilder)super.withMavenLibrary(coordinates, repo, exclusions);
    }

    @Override
    public AutomatedPythonJobBuilder withMavenLibrary(String coordinates, String[] exclusions) {
        return (AutomatedPythonJobBuilder)super.withMavenLibrary(coordinates, exclusions);
    }

    @Override
    public AutomatedPythonJobBuilder withPyPiLibrary(String packageName)  {
        return (AutomatedPythonJobBuilder)super.withPyPiLibrary(packageName);
    }

    @Override
    public AutomatedPythonJobBuilder withPyPiLibrary(String packageName, String repo) {
        return (AutomatedPythonJobBuilder)super.withPyPiLibrary(packageName, repo);
    }

    @Override
    public AutomatedPythonJobBuilder withCranLibrary(String packageName) {
        return (AutomatedPythonJobBuilder)super.withCranLibrary(packageName);
    }

    @Override
    public AutomatedPythonJobBuilder withCranLibrary(String packageName, String repo) {
        return (AutomatedPythonJobBuilder)super.withCranLibrary(packageName, repo);
    }

    @Override
    public AutomatedPythonJobBuilder withClusterSpec(ClusterSpec clusterSpec) {
        return (AutomatedPythonJobBuilder)super.withClusterSpec(clusterSpec);
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public AutomatedPythonJobBuilder withBaseParameter(String parameter) {
        _baseParameters.add(parameter);
        return this;
    }

    public AutomatedPythonJob create() throws JobConfigException {
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

            if(_baseParameters != null) {
                pythonTaskDTO.Parameters = _baseParameters.toArray(new String[_baseParameters.size()]);
            }

            jobSettingsDTO.SparkPythonTask = pythonTaskDTO;
            validate(jobSettingsDTO);

            return new AutomatedPythonJob(_client, _pythonScript, jobSettingsDTO, getLibraries());
        } catch (HttpException e) {
            throw new JobConfigException(e);
        } catch (IOException e) {
            throw new JobConfigException(e);
        } catch (LibraryConfigException e) {
            throw new JobConfigException(e);
        } catch (ResourceConfigException e) {
            throw new JobConfigException(e);
        }
    }
}
