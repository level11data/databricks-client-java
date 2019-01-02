package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.dbfs.*;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class DbfsClient extends AbstractDatabricksClient {

    private final String ENDPOINT_TARGET = "api/2.0/dbfs";

    public DbfsClient(DatabricksSession session) {
        super(session);
    }

    protected ClientConfig ClientConfig() {
        return super.ClientConfig();
    }

    private Client createClient() {
        return ClientBuilder.newClient(ClientConfig());
    }

    public FileInfoDTO getStatus(String path) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/get-status";

        Response response = Session.getRequestBuilder(pathSuffix,"path",path).get();

        checkResponse(response);
        return response.readEntity(FileInfoDTO.class);
    }

    public long create(CreateRequestDTO createRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/create";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(createRequestDTO));

        checkResponse(response);
        return response.readEntity(CreateResponseDTO.class).Handle;
    }

    public void close(CloseRequestDTO closeRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/close";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(closeRequestDTO));

        checkResponse(response);
    }

    public void addBlock(AddBlockRequestDTO addBlockRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/add-block";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(addBlockRequestDTO));

        checkResponse(response);
    }

    public void put(PutRequestDTO putRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/put";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(putRequestDTO));

        checkResponse(response);
    }

    public void delete(DbfsDeleteRequestDTO dbfsDeleteRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/delete";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(dbfsDeleteRequestDTO));

        checkResponse(response);
    }

    //TODO remove this and fold into super() class; why the need for another Exception Type??
    private void checkResponse(Response response, String message400) throws HttpException {
        // check response status code
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        } else {
            super.checkResponse(response);
        }
    }

    public DbfsListResponseDTO list(String path) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/list";

        Response response = Session.getRequestBuilder(pathSuffix, "path", path).get();

        checkResponse(response);
        return response.readEntity(DbfsListResponseDTO.class);
    }

    //TODO - Refactor to accept DTO
    public void mkdirs(String path) throws HttpException {
        DbfsMkdirsRequestDTO requestDTO = new DbfsMkdirsRequestDTO();
        requestDTO.Path = path;

        String pathSuffix = ENDPOINT_TARGET + "/mkdirs";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(requestDTO));

        checkResponse(response);
    }

    //TODO - Refactor to accept DTO
    public void move(String sourcePath, String destinationPath) throws HttpException {
        MoveRequestDTO requestDTO = new MoveRequestDTO();
        requestDTO.SourcePath = sourcePath;
        requestDTO.DestinationPath = destinationPath;

        String pathSuffix = ENDPOINT_TARGET + "/move";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(requestDTO));

        checkResponse(response);
    }

    public ReadResponseDTO read(String path, long offset, long length) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/read";

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("path", path);
        queryParams.put("offset", offset);
        queryParams.put("length",length);

        Response response = Session.getRequestBuilder(pathSuffix, queryParams).get();

        checkResponse(response);
        return response.readEntity(ReadResponseDTO.class);
    }
}
