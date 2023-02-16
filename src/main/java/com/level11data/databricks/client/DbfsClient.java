package com.level11data.databricks.client;

import java.util.Map;
import java.util.HashMap;
import com.level11data.databricks.client.entities.dbfs.ReadResponseDTO;
import com.level11data.databricks.client.entities.dbfs.MoveRequestDTO;
import com.level11data.databricks.client.entities.dbfs.DbfsMkdirsRequestDTO;
import com.level11data.databricks.client.entities.dbfs.DbfsListResponseDTO;
import com.level11data.databricks.client.entities.dbfs.DbfsDeleteRequestDTO;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.client.Client;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import javax.ws.rs.core.MediaType;
import java.io.File;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import javax.ws.rs.client.ClientBuilder;
import com.level11data.databricks.client.entities.dbfs.PutRequestDTO;
import com.level11data.databricks.client.entities.dbfs.AddBlockRequestDTO;
import com.level11data.databricks.client.entities.dbfs.CloseRequestDTO;
import com.level11data.databricks.client.entities.dbfs.CreateResponseDTO;
import javax.ws.rs.client.Entity;
import com.level11data.databricks.client.entities.dbfs.CreateRequestDTO;
import javax.ws.rs.core.Response;
import com.level11data.databricks.client.entities.dbfs.FileInfoDTO;
import com.level11data.databricks.session.WorkspaceSession;

public class DbfsClient extends AbstractDatabricksClient
{
    private final String ENDPOINT_TARGET = "api/2.0/dbfs";

    public DbfsClient(final WorkspaceSession session) {
        super(session);
    }

    public FileInfoDTO getStatus(final String path) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/get-status";
        final Response response = this.Session.getRequestBuilder(pathSuffix, "path", path).get();
        this.checkResponse(response);
        return response.readEntity(FileInfoDTO.class);
    }

    public long create(final CreateRequestDTO createRequestDTO) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/create";
        final Response response = this.Session.getRequestBuilder(pathSuffix).post(Entity.json(createRequestDTO));
        this.checkResponse(response);
        return response.readEntity(CreateResponseDTO.class).Handle;
    }

    public void close(final CloseRequestDTO closeRequestDTO) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/close";
        final Response response = this.Session.getRequestBuilder(pathSuffix).post(Entity.json(closeRequestDTO));
        this.checkResponse(response);
    }

    public void addBlock(final AddBlockRequestDTO addBlockRequestDTO) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/add-block";
        final Response response = this.Session.getRequestBuilder(pathSuffix).post(Entity.json(addBlockRequestDTO));
        this.checkResponse(response);
    }

    public void put(final PutRequestDTO putRequestDTO) throws HttpException {
        final String pathSuffix = String.format("%s/%s/put", this.Session.getEndpoint(), "api/2.0/dbfs");
        final Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
        final WebTarget server = client.target(pathSuffix);
        final MultiPart multiPart = new MultiPart();
        final FileDataBodyPart fileOption = new FileDataBodyPart("file", new File(putRequestDTO.Contents), MediaType.MULTIPART_FORM_DATA_TYPE);
        final FormDataBodyPart dbfsPathOption = new FormDataBodyPart("path", putRequestDTO.Path);
        final FormDataBodyPart overwiteOption = new FormDataBodyPart("overwrite", putRequestDTO.Overwrite + "");
        multiPart.bodyPart((BodyPart)fileOption);
        multiPart.bodyPart((BodyPart)dbfsPathOption);
        multiPart.bodyPart((BodyPart)overwiteOption);
        final Response response = server.request("multipart/form-data").header("Authorization", String.format("Bearer %s", this.Session.getToken())).header("User-Agent", this.Session.getUserAgent()).post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE));
        this.checkResponse(response);
    }

    public void delete(final DbfsDeleteRequestDTO dbfsDeleteRequestDTO) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/delete";
        final Response response = this.Session.getRequestBuilder(pathSuffix).post(Entity.json(dbfsDeleteRequestDTO));
        this.checkResponse(response);
    }

    private void checkResponse(final Response response, final String message400) throws HttpException {
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        }
        super.checkResponse(response);
    }

    public DbfsListResponseDTO list(final String path) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/list";
        final Response response = this.Session.getRequestBuilder(pathSuffix, "path", path).get();
        this.checkResponse(response);
        return response.readEntity(DbfsListResponseDTO.class);
    }

    public void mkdirs(final String path) throws HttpException {
        final DbfsMkdirsRequestDTO requestDTO = new DbfsMkdirsRequestDTO();
        requestDTO.Path = path;
        final String pathSuffix = "api/2.0/dbfs/mkdirs";
        final Response response = this.Session.getRequestBuilder(pathSuffix).post(Entity.json(requestDTO));
        this.checkResponse(response);
    }

    public void move(final String sourcePath, final String destinationPath) throws HttpException {
        final MoveRequestDTO requestDTO = new MoveRequestDTO();
        requestDTO.SourcePath = sourcePath;
        requestDTO.DestinationPath = destinationPath;
        final String pathSuffix = "api/2.0/dbfs/move";
        final Response response = this.Session.getRequestBuilder(pathSuffix).post(Entity.json(requestDTO));
        this.checkResponse(response);
    }

    public ReadResponseDTO read(final String path, final long offset, final long length) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/read";
        final Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("path", path);
        queryParams.put("offset", offset);
        queryParams.put("length", length);
        final Response response = this.Session.getRequestBuilder(pathSuffix, queryParams).get();
        this.checkResponse(response);
        return response.readEntity(ReadResponseDTO.class);
    }

    public ReadResponseDTO read(final String path, final long offset) throws HttpException {
        final String pathSuffix = "api/2.0/dbfs/read";
        final Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("path", path);
        queryParams.put("offset", offset);
        final Response response = this.Session.getRequestBuilder(pathSuffix, queryParams).get();
        this.checkResponse(response);
        return response.readEntity(ReadResponseDTO.class);
    }
}
