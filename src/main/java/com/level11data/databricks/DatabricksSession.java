package com.level11data.databricks;


import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.cluster.Cluster;
import com.level11data.databricks.cluster.ClusterIter;
import com.level11data.databricks.cluster.ClusterBuilder;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.entities.clusters.NodeTypeDTO;
import com.level11data.databricks.entities.clusters.SparkVersionDTO;
import com.level11data.databricks.entities.clusters.SparkVersionsDTO;

import java.util.*;

public class DatabricksSession {

    private DatabricksClientConfiguration _databricksClientConfig;
    private ClustersClient _clustersClient;
    private LibrariesClient _librariesClient;
    private SparkVersionsDTO _sparkVersions;

    public DatabricksSession(DatabricksClientConfiguration databricksConfig) {
        _databricksClientConfig = databricksConfig;
    }

    private ClustersClient getOrCreateClustersClient() {
        if(_clustersClient == null) {
            _clustersClient =  new ClustersClient(_databricksClientConfig);
        }
        return _clustersClient;
    }

    private LibrariesClient getOrCreateLibrariesClient() {
        if(_librariesClient == null) {
            _librariesClient =  new LibrariesClient(_databricksClientConfig);
        }
        return _librariesClient;
    }

    public ClusterBuilder createCluster(String name, Integer numWorkers)  {
        return new ClusterBuilder(getOrCreateClustersClient(), name, numWorkers);
    }

    public ClusterBuilder createCluster(String name, Integer minWorkers, Integer maxWorkers) {
        return new ClusterBuilder(getOrCreateClustersClient(), name, minWorkers, maxWorkers);
    }

    public String getDefaultSparkVersion() throws HttpException  {
        if(_sparkVersions == null) {
            _sparkVersions = getOrCreateClustersClient().getSparkVersions();
        }
        return _sparkVersions.DefaultVersionKey;
    }

    public Map<String,String> getSparkVersions() throws HttpException  {
        if(_sparkVersions == null) {
            _sparkVersions = getOrCreateClustersClient().getSparkVersions();
        }

        List<SparkVersionDTO> versionList = _sparkVersions.Versions;
        HashMap<String,String> versionMap = new HashMap<String,String>();

        for(SparkVersionDTO sv : versionList) {
            versionMap.put(sv.Key, sv.Name);
        }
        return versionMap;
    }

    public String getDefaultNodeType() throws HttpException {
        return getOrCreateClustersClient().getNodeTypes().DefaultNodeTypeId;
    }

    public List<com.level11data.databricks.cluster.NodeType> getNodeTypes() throws HttpException  {
        List<NodeTypeDTO> nodeTypesInfoListDTO
                = getOrCreateClustersClient().getNodeTypes().NodeTypes;

        ArrayList<com.level11data.databricks.cluster.NodeType> nodeTypesList
                = new ArrayList<com.level11data.databricks.cluster.NodeType>();

        for(NodeTypeDTO nt : nodeTypesInfoListDTO) {
            nodeTypesList.add(new com.level11data.databricks.cluster.NodeType(nt));
        }
        return nodeTypesList;
    }

    public String getDefaultZone()  throws HttpException {
        return getOrCreateClustersClient().getZones().DefaultZone;
    }

    public String[] getZones() throws HttpException  {
        return getOrCreateClustersClient().getZones().Zones;
    }

    /*
    public List<Cluster> listClusters() throws ClusterConfigException, HttpException {
        ClustersClient client = getOrCreateClustersClient();
        ClusterInfoDTO[] clusterInfos = client.listClusters().ClustersDTO;

        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        for(ClusterInfoDTO clusterInfo : clusterInfos) {
            clusters.add(new Cluster(client, clusterInfo));
        }
        return clusters;
    }
    */

    public Iterator<Cluster> listClusters() throws HttpException {
        ClustersClient client = getOrCreateClustersClient();
        ClusterInfoDTO[] clusterInfoDTOs = client.listClusters().Clusters;
        return new ClusterIter(client, clusterInfoDTOs);
    }

    public Cluster getCluster(String id) throws ClusterConfigException, HttpException {
        ClustersClient client = getOrCreateClustersClient();
        ClusterInfoDTO clusterInfoDTO = client.getCluster(id);
        return new Cluster(client, clusterInfoDTO);
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
