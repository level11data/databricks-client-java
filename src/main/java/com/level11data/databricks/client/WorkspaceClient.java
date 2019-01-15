package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.workspace.*;
import com.level11data.databricks.session.WorkspaceSession;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class WorkspaceClient extends AbstractDatabricksClient {
    private final String ENDPOINT_TARGET = "api/2.0/workspace";

    public WorkspaceClient(WorkspaceSession session) {
        super(session);
    }

    //TODO evaluate if this is necessary and remove.  Why is a new Excption type needed?  If so, fold it into super()
    private void checkResponse(Response response, String message400) throws HttpException {
        // check response status code
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        } else {
            super.checkResponse(response);
        }
    }

    public void delete(WorkspaceDeleteRequestDTO workspaceDeleteRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/delete";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(workspaceDeleteRequestDTO));

        checkResponse(response);
    }

    public ExportResponseDTO exportResource(ExportRequestDTO exportRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/export";

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("path",exportRequestDTO.Path);
        queryParams.put("format",exportRequestDTO.Format);

        Response response = Session.getRequestBuilder(pathSuffix, queryParams).get();

        checkResponse(response);
        return response.readEntity(ExportResponseDTO.class);
    }

    public StatusResponseDTO getStatus(StatusRequestDTO statusRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/get-status";

        Response response = Session.getRequestBuilder(pathSuffix, "path", statusRequestDTO.Path).get();

        checkResponse(response);
        return response.readEntity(StatusResponseDTO.class);
    }

    public void importResource(ImportRequestDTO importRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/import";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(importRequestDTO));

        checkResponse(response);
    }

    public WorkspaceListResponseDTO list(ListRequestDTO listRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/list";

        Response response = Session.getRequestBuilder(pathSuffix, "path", listRequestDTO.Path).get();

        checkResponse(response);
        return response.readEntity(WorkspaceListResponseDTO.class);
    }

    public void mkdirs(WorkspaceMkdirsRequestDTO workspaceMkdirsRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/mkdirs";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(workspaceMkdirsRequestDTO));

        // check response status code
        checkResponse(response);
    }
}
