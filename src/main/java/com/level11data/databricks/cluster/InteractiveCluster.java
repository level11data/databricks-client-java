package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.clusters.AutoScaleDTO;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.libraries.*;
import com.level11data.databricks.job.PythonScript;
import com.level11data.databricks.job.builder.InteractiveJarJobBuilder;
import com.level11data.databricks.job.builder.InteractiveNotebookJobBuilder;
import com.level11data.databricks.job.builder.InteractivePythonJobBuilder;
import com.level11data.databricks.library.*;
import com.level11data.databricks.library.util.LibraryHelper;
import com.level11data.databricks.workspace.Notebook;

import java.io.File;
import java.net.URISyntaxException;
import java.util.*;

public class InteractiveCluster extends AbstractCluster implements Cluster {
    private ClustersClient _client;
    private LibrariesClient _librariesClient;
    private JobsClient _jobsClient;
    private ArrayList<ClusterLibrary> _libraries = new ArrayList<>();


    private final Integer _autoTerminationMinutes;


    /**
     * Represents a Databricks InteractiveCluster.
     *
     * Is instantiated by
     *   1. InteractiveClusterBuilder.create()
     *   2. DatabricksSession.getCluster() by Id
     *   3. InteractiveCluster.resize()
     *
     * @param client Databricks ClusterClient
     * @param info Databricks ClusterInfoDTO POJO
     * @throws ClusterConfigException
     */
    public InteractiveCluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException {
        super(client, info);
        _client = client;

        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(info);

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        _autoTerminationMinutes = initAutoTerminationMinutes();
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.ClusterName == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have Name");
        }
    }

    private Integer initAutoTerminationMinutes() throws ClusterConfigException  {
        return getClusterInfo().AutoTerminationMinutes;
    }

    public void start() throws HttpException {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.ClusterId = getId();
        _client.start(clusterInfoDTO);
    }

    public void restart() throws HttpException {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.ClusterId = getId();
        _client.reStart(clusterInfoDTO);
    }

    public void terminate() throws HttpException {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.ClusterId = getId();
        _client.delete(clusterInfoDTO);
    }

    public InteractiveCluster resize(Integer numWorkers) throws ClusterConfigException, HttpException {
        if(IsAutoScaling) {
            throw new ClusterConfigException("Must Include New Min and Max Worker Values when Resizing an Autoscaling InteractiveCluster");
        }

        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.ClusterId = getId();
        clusterInfoDTO.NumWorkers = numWorkers;

        _client.resize(clusterInfoDTO);

        ClusterInfoDTO resizedClusterConfig = getClusterInfo();
        resizedClusterConfig.NumWorkers = numWorkers;
        return new InteractiveCluster(_client, resizedClusterConfig);
    }

    public InteractiveCluster resize(Integer minWorkers, Integer maxWorkers) throws ClusterConfigException, HttpException {
        if(!IsAutoScaling) {
            throw new ClusterConfigException("Must Only Include a Single Value When Resizing a Fixed Size InteractiveCluster");
        }

        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.ClusterId = getId();

        AutoScaleDTO autoScaleDTOSettings = new AutoScaleDTO();
        autoScaleDTOSettings.MinWorkers = minWorkers;
        autoScaleDTOSettings.MaxWorkers = maxWorkers;

        clusterInfoDTO.AutoScale = autoScaleDTOSettings;


        _client.resize(clusterInfoDTO);

        ClusterInfoDTO resizedClusterConfig = getClusterInfo();
        resizedClusterConfig.AutoScale.MinWorkers = minWorkers;
        resizedClusterConfig.AutoScale.MaxWorkers = maxWorkers;
        return new InteractiveCluster(_client, resizedClusterConfig);
    }

    private JobsClient getOrCreateJobsClient() {
        if(_jobsClient == null) {
            _jobsClient = new JobsClient(_client.Session);
        }
        return _jobsClient;
    }

    public InteractiveNotebookJobBuilder createJob(Notebook notebook) {
      return new InteractiveNotebookJobBuilder(getOrCreateJobsClient(), this, notebook);
    }

    public InteractiveNotebookJobBuilder createJob(Notebook notebook, Map<String,String> baseParameters) {
        return new InteractiveNotebookJobBuilder(getOrCreateJobsClient(), this, notebook, baseParameters);
    }

    public InteractiveJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName) {
        return new InteractiveJarJobBuilder(getOrCreateJobsClient(), this, jarLibrary, mainClassName);
    }

    public InteractiveJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName,
                                              File jarFile) {
        return new InteractiveJarJobBuilder(getOrCreateJobsClient(), this, jarLibrary, jarFile, mainClassName);
    }

    public InteractiveJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName,
                                              List<String> baseParameters) {
        return new InteractiveJarJobBuilder(getOrCreateJobsClient(), this, jarLibrary, mainClassName, baseParameters);
    }

    public InteractiveJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName,
                                              File jarFile,
                                              List<String> baseParameters) {
        return new InteractiveJarJobBuilder(getOrCreateJobsClient(), this, jarLibrary, jarFile, mainClassName, baseParameters);
    }

    public InteractivePythonJobBuilder createJob(PythonScript pythonScript, File pythonFile, List<String> params) {
        return new InteractivePythonJobBuilder(getOrCreateJobsClient(), this, pythonScript, pythonFile, params);
    }

    public InteractivePythonJobBuilder createJob(PythonScript pythonScript, File pythonFile) {
        return new InteractivePythonJobBuilder(getOrCreateJobsClient(), this, pythonScript, pythonFile);
    }

    public InteractivePythonJobBuilder createJob(PythonScript pythonScript) {
        return new InteractivePythonJobBuilder(getOrCreateJobsClient(), this, pythonScript);
    }

    private LibrariesClient getLibrariesClient() {
        if(_librariesClient == null) {
            _librariesClient = new LibrariesClient(_client.Session);
        }
        return _librariesClient;
    }

    public void installLibrary(AbstractLibrary library) throws ClusterConfigException {
        try {
            _libraries.add(library.install(this));
        } catch(LibraryConfigException e) {
            throw new ClusterConfigException(e);
        }

    }

    public void uninstallLibrary(AbstractLibrary library) throws ClusterConfigException {
        try {
            library.uninstall(this);
        } catch(LibraryConfigException e) {
            throw new ClusterConfigException(e);
        }
    }

    public List<ClusterLibrary> getLibraries() throws LibraryConfigException, HttpException, URISyntaxException {
        //add remote libraries to cached list (if NOT already in list)
        ClusterLibraryStatusesDTO remoteLibStatuses = getLibrariesClient().getClusterStatus(getId());

        if(remoteLibStatuses.LibraryStatuses == null) {
            //reset list of libraries
            _libraries = new ArrayList<>();
        } else {
            for (LibraryFullStatusDTO libStat : remoteLibStatuses.LibraryStatuses) {
                AbstractLibrary library = LibraryHelper.createLibrary(getLibrariesClient(), libStat.Library);
                ClusterLibrary clusterLibrary = getClusterLibraryFromCache(library);
                if(clusterLibrary == null) {
                    _libraries.add(new ClusterLibrary(this, library));
                }
            }
            //remove ClusterLibrary from cached list if it is NOT in the remote library status list
            //avoid ConcurrentModificationException by using the list's iterator
            Iterator<ClusterLibrary> iterClusterLibrary = _libraries.iterator();
            while(iterClusterLibrary.hasNext()) {
                ClusterLibrary clusterLib = iterClusterLibrary.next();
                if(getLibraryFromDTO(remoteLibStatuses, clusterLib.Library) == null) {
                    iterClusterLibrary.remove();
                }
            }
        }
        return Collections.unmodifiableList(_libraries);
    }

    private ClusterLibrary getClusterLibraryFromCache(AbstractLibrary library) throws HttpException {
        for (ClusterLibrary clusterLibrary : _libraries) {
            if(clusterLibrary.Library.equals(library)) {
                return clusterLibrary;
            }
        }
        //no ClusterLibrary matches the library
        return null;
    }

    private AbstractLibrary getLibraryFromDTO(ClusterLibraryStatusesDTO remoteLibStatuses, AbstractLibrary library) throws HttpException {
        for (LibraryFullStatusDTO remoteLibStatus : remoteLibStatuses.LibraryStatuses) {
            if (library.equals(remoteLibStatus.Library)) {
                return library;
            }
        }
        //AbstractLibrary could not be found in DTO
        return null;
    }

    public int getAutoTerminationMinutes() {
        return _autoTerminationMinutes;
    }
}
