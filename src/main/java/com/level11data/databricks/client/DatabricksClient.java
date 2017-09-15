package com.level11data.databricks.client;

import com.level11data.databricks.config.DatabricksClientConfiguration;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.core.Response;

public class DatabricksClient {
    protected HttpAuthenticationFeature _auth;

    public DatabricksClient(DatabricksClientConfiguration databricksConfig) {
        _auth = HttpAuthenticationFeature.basicBuilder()
                .credentials(databricksConfig.getClientUsername(), databricksConfig.getClientPassword())
                .build();
    }

    protected ClientConfig ClientConfig() {
        return new ClientConfig().register(new JacksonFeature());
    }

    protected void checkResponse(Response response) throws HttpException {
        // check response status code
        if (response.getStatus() == 401) {
            throw new HttpException("HTTP 401 Unauthorized: Not Authenticated");
        } else if(response.getStatus() == 403) {
            throw new HttpException("HTTP 403 Forbidden: Not Authorized");
        } else if (response.getStatus() != 200) {
            throw new HttpException("HTTP "+ response.getStatus() + ":");
        }
        //This will print the entire response body; useful for debugging code
        //String body = response.readEntity(String.class);
        //System.out.println(body);
    }

}
