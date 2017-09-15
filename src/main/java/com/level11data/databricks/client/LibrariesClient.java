package com.level11data.databricks.client;

import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.entities.libraries.AllClusterLibraryStatuses;
import com.level11data.databricks.entities.libraries.ClusterLibraryStatuses;
import com.level11data.databricks.entities.libraries.ClusterLibraryRequest;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class LibrariesClient extends DatabricksClient {

    private WebTarget _target;

    public LibrariesClient(DatabricksClientConfiguration databricksConfig) {
        super(databricksConfig);
        _target = createClient().target(databricksConfig.getClientUrl())
                .path("api").path("2.0").path("libraries");
    }

    protected ClientConfig ClientConfig() {
        return super.ClientConfig();
    }

    private Client createClient() {
        return ClientBuilder.newClient(ClientConfig());
    }

    public AllClusterLibraryStatuses getAllClusterStatuses() throws HttpException  {
        Response response = _target.path("all-cluster-statuses")
                .register(_auth)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(AllClusterLibraryStatuses.class);
    }

    public ClusterLibraryStatuses getClusterStatus(String clusterId) throws HttpException {
        Response response = _target.path("cluster-status")
                .register(_auth)
                .queryParam("cluster_id", clusterId)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(ClusterLibraryStatuses.class);
    }

    public void installLibraries(ClusterLibraryRequest clusterLibrariesRequest) throws HttpException {
        Response response = _target.path("install")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(clusterLibrariesRequest));

        checkResponse(response);
    }

    public void uninstallLibraries(ClusterLibraryRequest clusterLibrariesRequest) throws HttpException {
        Response response = _target.path("uninstall")
                .register(_auth)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(clusterLibrariesRequest));

        checkResponse(response);
    }


}
