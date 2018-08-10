package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.jobs.*;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class JobsClient extends DatabricksClient {
    private WebTarget _target;

    public JobsClient(DatabricksSession session) {
        super(session);
        _target = createClient().target(session.Url)
                .path("api").path("2.0").path("jobs");
    }

    protected ClientConfig ClientConfig() {
        return super.ClientConfig();
    }

    private Client createClient() {
        return ClientBuilder.newClient(ClientConfig());
    }

    public JobsDTO listJobs() throws HttpException  {
        Response response = _target.path("list")
                .register(Session.Authentication)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(JobsDTO.class);
    }

    public JobDTO getJob(long jobId) throws HttpException {
        //TODO should be DEBUG logging statement
        //System.out.println("getJob HTTP request for id "+jobId);
        Response response = _target.path("get")
                .register(Session.Authentication)
                .queryParam("job_id", jobId)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(JobDTO.class);
    }

    public long createJob(JobSettingsDTO jobSettingsDTO) throws HttpException {
        Response response = _target.path("create")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(jobSettingsDTO));

        checkResponse(response);
        return response.readEntity(CreateJobResponseDTO.class).JobId;
    }

    public void deleteJob(JobDTO jobDTO) throws HttpException {
        Response response = _target.path("delete")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(jobDTO));
    }

    public void resetJob(ResetJobRequestDTO resetJobRequestDTO) throws HttpException {
        Response response = _target.path("reset")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(resetJobRequestDTO));
    }

    public RunNowResponseDTO runJobNow(RunNowRequestDTO runNowRequestDTO) throws HttpException {
        Response response = _target.path("run-now")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(runNowRequestDTO));

        checkResponse(response);
        return response.readEntity(RunNowResponseDTO.class);
    }

    public RunsSubmitResponseDTO submitRun(RunsSubmitRequestDTO runsSubmitRequestDTO) throws HttpException {
        Response response = _target.path("runs/submit")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(runsSubmitRequestDTO));

        checkResponse(response);
        return response.readEntity(RunsSubmitResponseDTO.class);
    }

    public RunsDTO listRuns() throws HttpException  {
        Response response = _target.path("runs/list")
                .register(Session.Authentication)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(RunsDTO.class);
    }

    public RunDTO getRun(long runId) throws HttpException {
        //TODO should be DEBUG logging statement
        //System.out.println("getRun HTTP request for id "+runId);
        Response response = _target.path("runs/get")
                .register(Session.Authentication)
                .queryParam("run_id", runId)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(RunDTO.class);
    }

    public void cancelRun(RunDTO runDTO) throws HttpException {
        Response response = _target.path("runs/cancel")
                .register(Session.Authentication)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(runDTO));
    }

    public JobRunOutputDTO getRunOutput(long runId) throws HttpException {
        Response response = _target.path("runs/get-output")
                .register(Session.Authentication)
                .queryParam("run_id", runId)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get();

        checkResponse(response);
        return response.readEntity(JobRunOutputDTO.class);
    }

}
