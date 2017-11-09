package com.level11data.databricks.client;

import com.level11data.databricks.cluster.*;
import com.level11data.databricks.cluster.builder.InteractiveClusterBuilder;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.client.entities.jobs.JobDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.clusters.*;
import com.level11data.databricks.job.*;
import com.level11data.databricks.job.builder.AutomatedNotebookJobBuilder;
import com.level11data.databricks.workspace.Notebook;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import java.net.URI;
import java.util.*;

public class DatabricksSession {
    protected final HttpAuthenticationFeature Authentication;
    protected final URI Url;

    private final DatabricksClientConfiguration _databricksClientConfig;
    private ClustersClient _clustersClient;
    private JobsClient _jobsClient;
    private LibrariesClient _librariesClient;
    private SparkVersionsDTO _sparkVersionsDTO;
    private NodeTypesDTO _nodeTypesDTO;
    private List<SparkVersion> _sparkVersions;
    private List<NodeType> _nodeTypes;

    public DatabricksSession(DatabricksClientConfiguration databricksConfig) {
        _databricksClientConfig = databricksConfig;

        Authentication = HttpAuthenticationFeature.basicBuilder()
                .credentials(databricksConfig.getClientUsername(), databricksConfig.getClientPassword())
                .build();

        Url = databricksConfig.getClientUrl();
    }

    private ClustersClient getOrCreateClustersClient() {
        if(_clustersClient == null) {
            _clustersClient =  new ClustersClient(this);
        }
        return _clustersClient;
    }

    private LibrariesClient getOrCreateLibrariesClient() {
        if(_librariesClient == null) {
            _librariesClient =  new LibrariesClient(this);
        }
        return _librariesClient;
    }

    private JobsClient getOrCreateJobsClient() {
        if(_jobsClient == null) {
            _jobsClient = new JobsClient(this);
        }
        return _jobsClient;
    }

    public InteractiveClusterBuilder createCluster(String name, Integer numWorkers)  {
        return new InteractiveClusterBuilder(getOrCreateClustersClient(), name, numWorkers);
    }

    public InteractiveClusterBuilder createCluster(String name, Integer minWorkers, Integer maxWorkers) {
        return new InteractiveClusterBuilder(getOrCreateClustersClient(), name, minWorkers, maxWorkers);
    }

    private void refreshSparkVersionsDTO() throws HttpException {
        _sparkVersionsDTO = getOrCreateClustersClient().getSparkVersions();
    }

    private SparkVersionsDTO getOrRequestSparkVersionsDTO() throws HttpException {
        if(_sparkVersionsDTO == null) {
            refreshSparkVersionsDTO();
        }
        return _sparkVersionsDTO;
    }

    public SparkVersion getDefaultSparkVersion() throws HttpException, ClusterConfigException  {
        return getSparkVersionByKey(getOrRequestSparkVersionsDTO().DefaultVersionKey);
    }

    private void initSparkVersions() throws HttpException {
        String defaultSparkVersionKey = getOrRequestSparkVersionsDTO().DefaultVersionKey;
        boolean isDefaultKeyInList = false;

        List<SparkVersionDTO> sparkVersionsDTO = getOrRequestSparkVersionsDTO().Versions;
        ArrayList<SparkVersion> sparkVersions = new ArrayList<SparkVersion>();

        for(SparkVersionDTO svDTO : sparkVersionsDTO) {
            sparkVersions.add(new SparkVersion(svDTO.Key, svDTO.Name));
            if(svDTO.Key.equals(defaultSparkVersionKey)) {
                isDefaultKeyInList = true;
            }
        }
        //It's possible that the Default Spark Version is not included in the list
        // possibly because it is deprecated.  If so, add it to the list with the key
        // as both the key and the value (since the value cannot be derived)
        if(!isDefaultKeyInList) {
            sparkVersions.add(new SparkVersion(defaultSparkVersionKey, defaultSparkVersionKey));
        }
        _sparkVersions = sparkVersions;
    }

    public List<SparkVersion> getSparkVersions() throws HttpException  {
        if(_sparkVersions == null) {
            initSparkVersions();
        }
        return _sparkVersions;
    }

    public SparkVersion getSparkVersionByKey(String key) throws HttpException, ClusterConfigException {
        List<SparkVersion> sparkVersions = getSparkVersions();
        for (SparkVersion sv : sparkVersions) {
            if(sv.Key.equals(key)) {
                return sv;
            }
        }
        throw new ClusterConfigException("No SparkVersion Found For Key "+key);
    }

    private void refreshNodeTypesDTO() throws HttpException {
        _nodeTypesDTO = getOrCreateClustersClient().getNodeTypes();
    }

    private NodeTypesDTO getOrRequestNodeTypesDTO() throws HttpException {
        if(_nodeTypesDTO == null) {
            refreshNodeTypesDTO();
        }
        return _nodeTypesDTO;
    }

    public NodeType getDefaultNodeType() throws HttpException, ClusterConfigException {
        return getNodeTypeById(getOrRequestNodeTypesDTO().DefaultNodeTypeId);
    }

    private void initNodeTypes() throws HttpException {
        List<NodeTypeDTO> nodeTypesInfoListDTO
                = getOrCreateClustersClient().getNodeTypes().NodeTypes;

        ArrayList<NodeType> nodeTypesList = new ArrayList<NodeType>();

        for(NodeTypeDTO nt : nodeTypesInfoListDTO) {
            nodeTypesList.add(new NodeType(nt));
        }
        _nodeTypes = nodeTypesList;
    }

    public List<NodeType> getNodeTypes() throws HttpException  {
        if(_nodeTypes == null) {
            initNodeTypes();
        }
        return _nodeTypes;
    }

    public NodeType getNodeTypeById(String id) throws HttpException, ClusterConfigException {
        List<NodeType> nodeTypes = getNodeTypes();

        for (NodeType nt : nodeTypes) {
            if(nt.Id.equals(id)) {
                return nt;
            }
        }

        //No NodeTypeDTO found
        throw new ClusterConfigException("No NodeType Found For Id "+id);
    }

    public String getDefaultZone()  throws HttpException {
        return getOrCreateClustersClient().getZones().DefaultZone;
    }

    public String[] getZones() throws HttpException  {
        return getOrCreateClustersClient().getZones().Zones;
    }

    public Iterator<InteractiveCluster> listClusters() throws HttpException {
        ClustersClient client = getOrCreateClustersClient();
        ClusterInfoDTO[] clusterInfoDTOs = client.listClusters().Clusters;
        return new ClusterIter(client, clusterInfoDTOs);
    }

    public InteractiveCluster getCluster(String id) throws ClusterConfigException, HttpException {
        ClustersClient client = getOrCreateClustersClient();
        ClusterInfoDTO clusterInfoDTO = client.getCluster(id);
        return new InteractiveCluster(client, clusterInfoDTO);
    }

    //TODO make return type generic
    public Job getJob(long jobId) throws HttpException, ClusterConfigException, Exception {
        JobsClient client = getOrCreateJobsClient();
        JobDTO jobDTO = client.getJob(jobId);

        if(jobDTO.isInteractive() && jobDTO.isNotebookJob()) {
            return new InteractiveNotebookJob(client, jobDTO);
        } else if(jobDTO.isAutomated() && jobDTO.isNotebookJob()) {
            return new AutomatedNotebookJob(client, jobDTO);
        } else {
            throw new Exception("Unsupported Job Type");  //TODO keep this?
        }
    }

    //TODO make return type generic
    public JobRun getRun(long runId) throws HttpException, Exception {
        JobsClient client = getOrCreateJobsClient();
        RunDTO runDTO = client.getRun(runId);

        if(runDTO.isInteractive() && runDTO.isNotebookJob()) {
            JobRun run = new InteractiveNotebookJobRun(client, runDTO);
            return run;
        } else {
            throw new Exception("Unsupported Job Type");  //TODO keep this?
        }
    }

    public AutomatedNotebookJobBuilder createJob(Notebook notebook) {
        JobsClient client = getOrCreateJobsClient();
        return new AutomatedNotebookJobBuilder(client, notebook);
    }

    public AutomatedNotebookJobBuilder createJob(Notebook notebook, Map<String,String> parameters) {
        JobsClient client = getOrCreateJobsClient();
        return new AutomatedNotebookJobBuilder(client, notebook, parameters);
    }

}
