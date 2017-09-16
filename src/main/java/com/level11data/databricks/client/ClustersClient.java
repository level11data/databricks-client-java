package com.level11data.databricks.client;

import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.entities.clusters.*;
import org.glassfish.jersey.client.ClientConfig;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class ClustersClient extends DatabricksClient {
    private WebTarget _target;

    public ClustersClient(DatabricksClientConfiguration databricksConfig) {
        super(databricksConfig);
        _target = createClient().target(databricksConfig.getClientUrl())
                .path("api").path("2.0").path("clusters");
    }

    protected ClientConfig ClientConfig() {
        return super.ClientConfig();
    }

    private Client createClient() {
        return ClientBuilder.newClient(ClientConfig());
    }

    public SparkVersionsDTO getSparkVersions() throws HttpException  {
        Response response = _target.path("spark-versions")
                .register(_auth)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(SparkVersionsDTO.class);
    }

    //TODO Default Node Type is still "Memory Optimized"
    public NodeTypesDTO getNodeTypes() throws HttpException  {
        Response response = _target.path("list-node-types")
                .register(_auth)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(NodeTypesDTO.class);
    }

    public ZonesDTO getZones() throws HttpException {
        Response response = _target.path("list-zones")
                .register(_auth)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(ZonesDTO.class);
    }

    public ClustersDTO listClusters() throws HttpException  {
        Response response = _target.path("list")
                .register(_auth)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(ClustersDTO.class);
    }

    public ClusterInfoDTO getCluster(String clusterId) throws HttpException {
        //TODO should be DEBUG logging statement
        System.out.println("getCluster HTTP request for id "+clusterId);
        Response response = _target.path("get")
                .register(_auth)
                .queryParam("cluster_id", clusterId)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(ClusterInfoDTO.class);
    }

    public void start(String clusterId) throws HttpException {
        ClusterInfoDTO cluster = new ClusterInfoDTO();
        cluster.ClusterId = clusterId;

        Response response = _target.path("start")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(cluster));

        // check response status code
        checkResponse(response, "Cluster " + clusterId + " is already started");
    }

    public void reStart(String clusterId) throws HttpException {
        ClusterInfoDTO cluster = new ClusterInfoDTO();
        cluster.ClusterId = clusterId;

        Response response = _target.path("restart")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(cluster));

        // check response status code
        checkResponse(response, "Cluster " + clusterId + " is not in a RUNNING state");
    }

    public void delete(String clusterId) throws HttpException {
        ClusterInfoDTO cluster = new ClusterInfoDTO();
        cluster.ClusterId = clusterId;

        Response response = _target.path("delete")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(cluster));

        // check response status code
        checkResponse(response, "Cluster " + clusterId + " is already TERMINATED or TERMINATING");
    }

    public void resize(String clusterId, Integer numWorkers) throws HttpException {
        ClusterInfoDTO cluster = new ClusterInfoDTO();
        cluster.ClusterId = clusterId;
        cluster.NumWorkers = numWorkers;

        Response response = _target.path("resize")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(cluster));

        // check response status code
        checkResponse(response, "Cluster " + clusterId + " is not in a RUNNING state");
    }

    public void resize(String clusterId, Integer minWorkers, Integer maxWorkers) throws HttpException {
        ClusterInfoDTO cluster = new ClusterInfoDTO();
        cluster.ClusterId = clusterId;

        AutoScaleDTO autoScaleDTOSettings = new AutoScaleDTO();
        autoScaleDTOSettings.MinWorkers = minWorkers;
        autoScaleDTOSettings.MaxWorkers = maxWorkers;

        cluster.AutoScale = autoScaleDTOSettings;

        Response response = _target.path("resize")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(cluster));

        // check response status code
        checkResponse(response, "Cluster " + clusterId + " is not in a RUNNING state");
    }

    public String create(ClusterInfoDTO clusterInfoDTO) throws HttpException {
        Response response = _target.path("create")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(clusterInfoDTO));

        checkResponse(response);
        return response.readEntity(CreateClusterResponseDTO.class).ClusterId;
    }

    private void checkResponse(Response response, String message400) throws HttpException {
        // check response status code
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        } else {
            super.checkResponse(response);
        }
    }

}
