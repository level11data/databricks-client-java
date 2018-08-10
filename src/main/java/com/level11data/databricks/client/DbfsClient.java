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

    public long create(CreateRequestDTO createRequestDTO) throws HttpException {
        Response response = _target.path("create")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(createRequestDTO));

        checkResponse(response);
        return response.readEntity(CreateResponseDTO.class).Handle;
    }

    public void close(CloseRequestDTO closeRequestDTO) throws HttpException {
        Response response = _target.path("close")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(closeRequestDTO));

        checkResponse(response);
    }

    public void addBlock(AddBlockRequestDTO addBlockRequestDTO) throws HttpException {
        Response response = _target.path("add-block")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(addBlockRequestDTO));

        checkResponse(response);
    }

    public void put(PutRequestDTO putRequestDTO) throws HttpException {
        Response response = _target.path("put")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(putRequestDTO));

        checkResponse(response);
    }

    public void delete(DbfsDeleteRequestDTO dbfsDeleteRequestDTO) throws HttpException {
//        DeleteRequestDTO deleteRequestDTO = new DeleteRequestDTO();
//        deleteRequestDTO.Path = path;
//        deleteRequestDTO.Recursive = recursive;

        Response response = _target.path("delete")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(dbfsDeleteRequestDTO));

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

    public DbfsListResponseDTO list(String path) throws HttpException {
        Response response = _target.path("list")
                .register(Session.Authentication)
                .queryParam("path", path)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(DbfsListResponseDTO.class);
    }

    public void mkdirs(String path) throws HttpException {
        DbfsMkdirsRequestDTO requestDTO = new DbfsMkdirsRequestDTO();
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
