package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.dbfs.*;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class DbfsClient extends DatabricksClient {

    private WebTarget _target;

    public DbfsClient(DatabricksSession session) {
        super(session);
        _target = createClient().target(Session.Url)
                .path("api").path("2.0").path("dbfs");
    }

    protected ClientConfig ClientConfig() {
        return super.ClientConfig();
    }

    private Client createClient() {
        return ClientBuilder.newClient(ClientConfig());
    }

    public FileInfoDTO getStatus(String path) throws HttpException {
        Response response = _target.path("get-status")
                .register(Session.Authentication)
                .queryParam("path", path)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(FileInfoDTO.class);
    }

    public long create(String path, boolean overwrite) throws HttpException {
        CreateRequestDTO requestDTO = new CreateRequestDTO();
        requestDTO.Path = path;
        requestDTO.Overwrite = overwrite;

        Response response = _target.path("create")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(requestDTO));

        checkResponse(response);
        return response.readEntity(CreateResponseDTO.class).Handle;
    }

    public void close(long handle) throws HttpException {
        CloseRequestDTO requestDTO = new CloseRequestDTO();
        requestDTO.Handle = handle;

        Response response = _target.path("close")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(requestDTO));

        checkResponse(response);
    }

    public void addBlock(long handle, byte[] data) throws HttpException {
        AddBlockRequestDTO requestDTO = new AddBlockRequestDTO();
        requestDTO.Handle = handle;
        requestDTO.Data = data;

        Response response = _target.path("add-block")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(requestDTO));

        checkResponse(response);
    }

    public void delete(String path, boolean recursive) throws HttpException {
        DeleteRequestDTO requestDTO = new DeleteRequestDTO();
        requestDTO.Path = path;
        requestDTO.Recursive = recursive;

        Response response = _target.path("delete")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(requestDTO));

        checkResponse(response);
    }

    private void checkResponse(Response response, String message400) throws HttpException {
        // check response status code
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        } else {
            super.checkResponse(response);
        }
    }

    public ListResponseDTO list(String path) throws HttpException {
        Response response = _target.path("list")
                .register(Session.Authentication)
                .queryParam("path", path)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(ListResponseDTO.class);
    }

    public void mkdirs(String path) throws HttpException {
        MkdirsRequestDTO requestDTO = new MkdirsRequestDTO();
        requestDTO.Path = path;

        Response response = _target.path("mkdirs")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(requestDTO));

        checkResponse(response);
    }

    public void move(String sourcePath, String destinationPath) throws HttpException {
        MoveRequestDTO requestDTO = new MoveRequestDTO();
        requestDTO.SourcePath = sourcePath;
        requestDTO.DestinationPath = destinationPath;

        Response response = _target.path("move")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(requestDTO));

        checkResponse(response);
    }

    public ReadResponseDTO read(String path, long offset, long length) throws HttpException {
        Response response = _target.path("read")
                .register(Session.Authentication)
                .queryParam("path", path)
                .queryParam("offset", offset)
                .queryParam("length", length)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(ReadResponseDTO.class);
    }
}
