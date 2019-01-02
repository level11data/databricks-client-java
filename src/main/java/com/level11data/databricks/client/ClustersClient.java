package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.clusters.*;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

public class ClustersClient extends AbstractDatabricksClient {
    private final String ENDPOINT_TARGET = "api/2.0/clusters";

    public ClustersClient(DatabricksSession session) {
        super(session);
    }

    public SparkVersionsDTO getSparkVersions() throws HttpException  {
        String pathSuffix = ENDPOINT_TARGET + "/spark-versions";

        Response response = Session.getRequestBuilder(pathSuffix).get();

        checkResponse(response);
        return response.readEntity(SparkVersionsDTO.class);
    }

    public NodeTypesDTO getNodeTypes() throws HttpException  {
        String pathSuffix = ENDPOINT_TARGET + "/list-node-types";

        Response response = Session.getRequestBuilder(pathSuffix).get();

        checkResponse(response);
        return response.readEntity(NodeTypesDTO.class);
    }

    public ZonesDTO getZones() throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/list-zones";

        Response response = Session.getRequestBuilder(pathSuffix).get();

        checkResponse(response);
        return response.readEntity(ZonesDTO.class);
    }

    public ClustersDTO listClusters() throws HttpException  {
        String pathSuffix = ENDPOINT_TARGET + "/list";

        Response response = Session.getRequestBuilder(pathSuffix).get();

        checkResponse(response);
        return response.readEntity(ClustersDTO.class);
    }

    public ClusterInfoDTO getCluster(String clusterId) throws HttpException {
        //TODO should be DEBUG logging statement  System.out.println("getCluster HTTP request for id "+clusterId);

        String pathSuffix = ENDPOINT_TARGET + "/get";

        Response response = Session.getRequestBuilder(pathSuffix, "cluster_id", clusterId).get();

        checkResponse(response);
        return response.readEntity(ClusterInfoDTO.class);
    }

    public void start(ClusterInfoDTO clusterInfoDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/start";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(clusterInfoDTO));

        // check response status code
        checkResponse(response, "InteractiveCluster " + clusterInfoDTO.ClusterId + " is already started");
    }

    public void reStart(ClusterInfoDTO clusterInfoDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/restart";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(clusterInfoDTO));

        // check response status code
        checkResponse(response, "InteractiveCluster " + clusterInfoDTO.ClusterId + " is not in a RUNNING state");
    }

    public void delete(ClusterInfoDTO clusterInfoDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/delete";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(clusterInfoDTO));

        // check response status code
        checkResponse(response, "InteractiveCluster " + clusterInfoDTO.ClusterId + " is already TERMINATED or TERMINATING");
    }

    public void resize(ClusterInfoDTO clusterInfoDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/resize";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(clusterInfoDTO));

        // check response status code
        checkResponse(response, "InteractiveCluster " + clusterInfoDTO.ClusterId + " is not in a RUNNING state");
    }

    public String create(ClusterInfoDTO clusterInfoDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/create";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(clusterInfoDTO));

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
