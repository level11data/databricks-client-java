package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.libraries.AllClusterLibraryStatusesDTO;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryRequestDTO;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryStatusesDTO;
import com.level11data.databricks.session.WorkspaceSession;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

public class LibrariesClient extends AbstractDatabricksClient {

    private final String ENDPOINT_TARGET = "api/2.0/libraries";

    public LibrariesClient(WorkspaceSession session) {
        super(session);
    }

    public AllClusterLibraryStatusesDTO getAllClusterStatuses() throws HttpException  {
        String pathSuffix = ENDPOINT_TARGET + "/all-cluster-statuses";

        Response response = Session.getRequestBuilder(pathSuffix).get();

        checkResponse(response);
        return response.readEntity(AllClusterLibraryStatusesDTO.class);
    }

    public ClusterLibraryStatusesDTO getClusterStatus(String clusterId) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/cluster-status";

        Response response = Session.getRequestBuilder(pathSuffix, "cluster_id", clusterId).get();

        checkResponse(response);
        return response.readEntity(ClusterLibraryStatusesDTO.class);
    }

    public void installLibraries(ClusterLibraryRequestDTO clusterLibrariesRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/install";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(clusterLibrariesRequestDTO));

        checkResponse(response);
    }

    public void uninstallLibraries(ClusterLibraryRequestDTO clusterLibrariesRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/uninstall";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(clusterLibrariesRequestDTO));

        checkResponse(response);
    }


}
