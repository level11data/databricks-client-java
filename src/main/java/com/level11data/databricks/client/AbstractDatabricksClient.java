package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.HttpErrorDTO;
import com.level11data.databricks.session.WorkspaceSession;

import javax.ws.rs.core.Response;

public abstract class AbstractDatabricksClient {
    public final WorkspaceSession Session;

    public AbstractDatabricksClient(WorkspaceSession session) {
        Session = session;
    }


    protected void checkResponse(Response response) throws HttpException {
        //This will print the entire response body; useful for debugging code
//        String debugBody = response.readEntity(String.class);
//        System.out.println(debugBody);

        // check response status code
        if (response.getStatus() == 400) {
            //bad request
            HttpErrorDTO httpErrorDTO = response.readEntity(HttpErrorDTO.class);
            throw new HttpException(httpErrorDTO.ErrorCode + " : " + httpErrorDTO.ErrorMessage);
        } else if (response.getStatus() == 401) {
            String body = response.readEntity(String.class);
            System.out.println("HTTP "+ response.getStatus() + ":" + body);
            throw new HttpException("HTTP 401 Unauthorized: Not Authenticated");
        } else if(response.getStatus() == 403) {
            throw new HttpException("HTTP 403 Forbidden: Not Authorized");
        } else if (response.getStatus() != 200) {
            String body = response.readEntity(String.class);
            throw new HttpException("HTTP "+ response.getStatus() + ":" + body);
        }
    }

}
