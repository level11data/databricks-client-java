package com.level11data.databricks.client;

import com.level11data.databricks.entities.contexts.CreateContextRequestDTO;
import com.level11data.databricks.entities.contexts.CreateContextResponseDTO;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class ContextsClient extends DatabricksClient {
    private WebTarget _target;

    public ContextsClient(DatabricksSession session) {
        super(session);
        _target = createClient().target(session.Url)
                .path("api").path("1.2").path("contexts");
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

    public long createContext(CreateContextRequestDTO contextRequestDTO) throws HttpException {
        Response response = _target.path("create")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(contextRequestDTO));

        checkResponse(response);
        return response.readEntity(CreateContextResponseDTO.class).Id;
    }
}
