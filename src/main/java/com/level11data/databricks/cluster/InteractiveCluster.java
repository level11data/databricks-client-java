package com.level11data.databricks.cluster;

import com.level11data.databricks.ClustersClient;
import com.level11data.databricks.HttpException;
import com.level11data.databricks.JobsClient;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.job.builder.InteractiveNotebookJobBuilder;
import com.level11data.databricks.workspace.Notebook;

import java.util.*;

public class InteractiveCluster extends Cluster{
    private ClustersClient _client;
    private Boolean _isAutoScaling = false;
    private JobsClient _jobsClient;

    public final String Name;
    public final Integer NumWorkers;
    public final AutoScale AutoScale;
    public final Integer AutoTerminationMinutes;

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
    public InteractiveCluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        super(client, info);
        _client = client;

        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(info);

        Name = info.ClusterName;
        NumWorkers = info.NumWorkers;

        if(info.AutoScale != null){
            _isAutoScaling = true;
            AutoScale = new AutoScale(info.AutoScale);
        } else {
            AutoScale = null;
        }

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        AutoTerminationMinutes = initAutoTerminationMinutes();
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.ClusterName == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have Name");
        }

        if(info.NumWorkers == null && info.AutoScale == null)  {
            throw new ClusterConfigException("ClusterInfoDTO Must Have either NumWorkers OR AutoScaleDTO");
        }
    }

    private Integer initAutoTerminationMinutes() throws HttpException  {
        return getClusterInfo().AutoTerminationMinutes;
    }

    public void start() throws HttpException {
        _client.start(Id);
    }

    public void restart() throws HttpException {
        _client.reStart(Id);
    }

    public void terminate() throws HttpException {
        _client.delete(Id);
    }

    public InteractiveCluster resize(Integer numWorkers) throws ClusterConfigException, HttpException {
        if(_isAutoScaling) {
            throw new ClusterConfigException("Must Include New Min and Max Worker Values when Resizing an Autoscaling InteractiveCluster");
        }
        _client.resize(Id, numWorkers);

        ClusterInfoDTO resizedClusterConfig = getClusterInfo();
        resizedClusterConfig.NumWorkers = numWorkers;
        return new InteractiveCluster(_client, resizedClusterConfig);
    }

    public InteractiveCluster resize(Integer minWorkers, Integer maxWorkers) throws ClusterConfigException, HttpException {
        if(!_isAutoScaling) {
            throw new ClusterConfigException("Must Only Include a Single Value When Resizing a Fixed Size InteractiveCluster");
        }
        _client.resize(Id, minWorkers, maxWorkers);

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
}
