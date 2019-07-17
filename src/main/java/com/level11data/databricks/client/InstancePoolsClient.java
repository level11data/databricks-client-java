package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.instancepools.*;
import com.level11data.databricks.session.WorkspaceSession;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

public class InstancePoolsClient extends AbstractDatabricksClient {

    private final String ENDPOINT_TARGET = "api/2.0/instance-pools";

    public InstancePoolsClient(WorkspaceSession session) {
        super(session);
    }

    public String createInstancePool(InstancePoolInfoDTO instancePoolInfoDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/create";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(instancePoolInfoDTO));

        checkResponse(response);
        return response.readEntity(InstancePoolIdDTO.class).InstancePoolId;
    }

    public boolean deleteInstancePool(InstancePoolIdDTO instancePoolIdDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/delete";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(instancePoolIdDTO));

        // check response status code
        checkResponse(response);

        return response.readEntity(InstancePoolDeleteResponseDTO.class).Deleted;
    }

    public boolean editInstancePool(InstancePoolEditRequestDTO instancePoolEditRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/edit";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(instancePoolEditRequestDTO));

        // check response status code
        checkResponse(response);

        return response.readEntity(InstancePoolEditResponseDTO.class).Edited;
    }

    public InstancePoolGetResponseDTO getInstancePool(String instancePoolId) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/get";

        Response response = Session.getRequestBuilder(pathSuffix, "instance_pool_id", instancePoolId).get();

        // check response status code
        checkResponse(response);

        return response.readEntity(InstancePoolGetResponseDTO.class);
    }

    public InstancePoolListResponseDTO listInstancePools(InstancePoolListRequestDTO instancePoolListRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/list";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(instancePoolListRequestDTO));

        // check response status code
        checkResponse(response);

        return response.readEntity(InstancePoolListResponseDTO.class);
    }

    public InstancePoolListResponseDTO listInstancePoolsByIds(InstancePoolListByIdRequestDTO instancePoolListByIdRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/list-by-ids";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(instancePoolListByIdRequestDTO));

        // check response status code
        checkResponse(response);

        return response.readEntity(InstancePoolListResponseDTO.class);
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
