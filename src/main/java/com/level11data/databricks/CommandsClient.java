package com.level11data.databricks;

import com.level11data.databricks.entities.commands.CommandRequestDTO;
import com.level11data.databricks.entities.commands.CommandResponseDTO;
import com.level11data.databricks.entities.commands.CommandStatusDTO;
import com.level11data.databricks.entities.commands.ExecuteCommandRequestDTO;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class CommandsClient extends DatabricksClient {
    private WebTarget _target;

    public CommandsClient(DatabricksSession session) {
        super(session);
        _target = createClient().target(session.Url)
                .path("api").path("1.2").path("commands");
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

    public String executeCommand(ExecuteCommandRequestDTO executeCommandRequestDTO) throws HttpException {
        Response response = _target.path("execute")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(executeCommandRequestDTO));

        checkResponse(response);
        return response.readEntity(CommandResponseDTO.class).Id;
    }

    public CommandStatusDTO getCommandStatus(CommandRequestDTO commandRequestDTO) throws HttpException {
        Response response = _target.path("status")
                .register(Session.Authentication)
                .queryParam("clusterId", commandRequestDTO.ClusterId)
                .queryParam("contextId", commandRequestDTO.ContextId)
                .queryParam("commandId", commandRequestDTO.CommandId)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get();

        checkResponse(response);
        return response.readEntity(CommandStatusDTO.class);
    }

    public void cancelCommand(CommandRequestDTO commandRequestDTO) throws HttpException {
        Response response = _target.path("cancel")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(commandRequestDTO));

        checkResponse(response);
    }
}
