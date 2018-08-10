package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.workspace.*;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class WorkspaceClient extends DatabricksClient {
    private WebTarget _target;

    public WorkspaceClient(DatabricksSession session) {
        super(session);
        _target = createClient().target(session.Url)
                .path("api").path("2.0").path("workspace");
    }

    protected ClientConfig ClientConfig() {
        return super.ClientConfig();
    }

    private Client createClient() {
        return ClientBuilder.newClient(ClientConfig());
    }

    private void checkResponse(Response response, String message400) throws HttpException {
        // check response status code
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        } else {
            super.checkResponse(response);
        }
    }

    public void delete(WorkspaceDeleteRequestDTO workspaceDeleteRequestDTO) throws HttpException {
        Response response = _target.path("delete")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(workspaceDeleteRequestDTO));

        // check response status code
        checkResponse(response);
    }

    public ExportResponseDTO exportResource(ExportRequestDTO exportRequestDTO) throws HttpException {
        Response response = _target.path("export")
                .register(Session.Authentication)
                .queryParam("path", exportRequestDTO.Path)
                .queryParam("format", exportRequestDTO.Format)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .get();

        // check response status code
        checkResponse(response);
        return response.readEntity(ExportResponseDTO.class);
    }

    public StatusResponseDTO getStatus(StatusRequestDTO statusRequestDTO) throws HttpException {
        Response response = _target.path("get-status")
                .register(Session.Authentication)
                .queryParam("path", statusRequestDTO.Path)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .get();

        // check response status code
        checkResponse(response);
        return response.readEntity(StatusResponseDTO.class);
    }

    public void importResource(ImportRequestDTO importRequestDTO) throws HttpException {
        Response response = _target.path("import")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(importRequestDTO));

        // check response status code
        checkResponse(response);
    }

    public WorkspaceListResponseDTO list(ListRequestDTO listRequestDTO) throws HttpException {
        Response response = _target.path("list")
                .register(Session.Authentication)
                .queryParam("path", listRequestDTO.Path)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .get();

        // check response status code
        checkResponse(response);
        return response.readEntity(WorkspaceListResponseDTO.class);
    }

    public void mkdirs(WorkspaceMkdirsRequestDTO workspaceMkdirsRequestDTO) throws HttpException {
        Response response = _target.path("mkdirs")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(workspaceMkdirsRequestDTO));

        // check response status code
        checkResponse(response);
    }
}
