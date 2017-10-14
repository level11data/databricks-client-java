package com.level11data.databricks.client;

import com.level11data.databricks.cluster.*;
import com.level11data.databricks.cluster.builder.InteractiveClusterBuilder;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.entities.clusters.*;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import java.net.URI;
import java.util.*;

public class DatabricksSession {
    protected final HttpAuthenticationFeature Authentication;
    protected final URI Url;

    private final DatabricksClientConfiguration _databricksClientConfig;
    private ClustersClient _clustersClient;
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
        //System.out.println("getSparkVersionByKey("+key+")");
        List<SparkVersion> sparkVersions = getSparkVersions();
        for (SparkVersion sv : sparkVersions) {
            //System.out.println("getSparkVersionByKey("+key+") Evaluate Key: "+sv.Key);
            if(sv.Key.equals(key)) {
                //System.out.println("getSparkVersionByKey("+key+") in IF");
                return sv;
            }
        }
        //System.out.println("getSparkVersionByKey("+key+") past FOR");
        //No SparkVersion found
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

    public void startCluster(String id) throws HttpException {
        getOrCreateClustersClient().start(id);
    }

    public void restartCluster(String id) throws HttpException {
        getOrCreateClustersClient().reStart(id);
    }

    public void deleteCluster(String id) throws HttpException {
        getOrCreateClustersClient().delete(id);
    }

    public void resizeCluster(String id, Integer numWorkers) throws HttpException {
        getOrCreateClustersClient().resize(id, numWorkers);
    }

    public void resizeCluster(String id, Integer minWorkers, Integer maxWorkers) throws HttpException {
        getOrCreateClustersClient().resize(id, minWorkers, maxWorkers);
    }


}
