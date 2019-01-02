package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.contexts.CreateContextRequestDTO;
import com.level11data.databricks.client.entities.contexts.CreateContextResponseDTO;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

public class ContextsClient extends AbstractDatabricksClient {
    private final String ENDPOINT_TARGET = "api/1.2/contexts";

    public ContextsClient(DatabricksSession session) {
        super(session);
    }

    protected ClientConfig ClientConfig() {
        return super.ClientConfig();
    }

    private Client createClient() {
        return ClientBuilder.newClient(ClientConfig());
    }

    //TODO re-evaluate if this is needed; if so, fold into super()
    private void checkResponse(Response response, String message400) throws HttpException {
        // check response status code
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        } else {
            super.checkResponse(response);
        }
    }

    public long createContext(CreateContextRequestDTO contextRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/create";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(contextRequestDTO));

        checkResponse(response);
        return response.readEntity(CreateContextResponseDTO.class).Id;
    }
}
