package com.level11data.databricks.client;

import javax.ws.rs.core.Response;

public abstract class AbstractDatabricksClient {
    public final DatabricksSession Session;

    public AbstractDatabricksClient(DatabricksSession session) {
        Session = session;
    }


    protected void checkResponse(Response response) throws HttpException {
        //This will print the entire response body; useful for debugging code
        //String debugBody = response.readEntity(String.class);
        //System.out.println(debugBody);

        // check response status code
        if (response.getStatus() == 400) {
            String body = response.readEntity(String.class);
            throw new HttpException("HTTP 400 Bad Request: " + body);
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
