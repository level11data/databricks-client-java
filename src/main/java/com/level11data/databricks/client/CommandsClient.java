package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.commands.CommandRequestDTO;
import com.level11data.databricks.client.entities.commands.CommandResponseDTO;
import com.level11data.databricks.client.entities.commands.CommandStatusDTO;
import com.level11data.databricks.client.entities.commands.ExecuteCommandRequestDTO;
import com.level11data.databricks.session.WorkspaceSession;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class CommandsClient extends AbstractDatabricksClient {
    private final String ENDPOINT_TARGET = "api/1.2/commands";

    public CommandsClient(WorkspaceSession session) {
        super(session);
    }

    //TODO - Reevaluate if this is needed; if so collapse into super()
    private void checkResponse(Response response, String message400) throws HttpException {
        // check response status code
        if (response.getStatus() == 400) {
            throw new HttpServerSideException(message400);
        } else {
            super.checkResponse(response);
        }
    }

    public String executeCommand(ExecuteCommandRequestDTO executeCommandRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/execute";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(executeCommandRequestDTO));

        checkResponse(response);
        return response.readEntity(CommandResponseDTO.class).Id;
    }

    public CommandStatusDTO getCommandStatus(CommandRequestDTO commandRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/status";

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("clusterId",commandRequestDTO.ClusterId);
        queryParams.put("contextId",commandRequestDTO.ContextId);
        queryParams.put("commandId",commandRequestDTO.CommandId);

        Response response = Session.getRequestBuilder(pathSuffix, queryParams).get();

        checkResponse(response);
        return response.readEntity(CommandStatusDTO.class);
    }

    public void cancelCommand(CommandRequestDTO commandRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/cancel";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(commandRequestDTO));

        checkResponse(response);
    }
}
