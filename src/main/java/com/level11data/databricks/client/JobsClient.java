package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.jobs.*;
import com.level11data.databricks.session.WorkspaceSession;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

public class JobsClient extends AbstractDatabricksClient {
    private final String ENDPOINT_TARGET = "api/2.0/jobs";

    public JobsClient(WorkspaceSession session) {
        super(session);
    }

    public JobsDTO listJobs() throws HttpException  {
        String pathSuffix = ENDPOINT_TARGET + "/list";

        Response response = Session.getRequestBuilder(pathSuffix).get();

        checkResponse(response);
        return response.readEntity(JobsDTO.class);
    }

    public JobDTO getJob(long jobId) throws HttpException {
        //TODO should be DEBUG logging statement
        //System.out.println("getJob HTTP request for id "+jobId);
        String pathSuffix = ENDPOINT_TARGET + "/get";

        Response response = Session.getRequestBuilder(pathSuffix, "job_id", jobId).get();

        checkResponse(response);
        return response.readEntity(JobDTO.class);
    }
    public long createJob(JobSettingsDTO jobSettingsDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/create";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(jobSettingsDTO));

        checkResponse(response);
        return response.readEntity(CreateJobResponseDTO.class).JobId;
    }

    public void deleteJob(JobDTO jobDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/delete";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(jobDTO));

        checkResponse(response);
    }

    public void resetJob(ResetJobRequestDTO resetJobRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/reset";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(resetJobRequestDTO));

        checkResponse(response);
    }

    public RunNowResponseDTO runJobNow(RunNowRequestDTO runNowRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/run-now";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(runNowRequestDTO));

        checkResponse(response);
        return response.readEntity(RunNowResponseDTO.class);
    }

    public RunsSubmitResponseDTO submitRun(RunsSubmitRequestDTO runsSubmitRequestDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/runs/submit";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(runsSubmitRequestDTO));

        checkResponse(response);
        return response.readEntity(RunsSubmitResponseDTO.class);
    }

    public RunsSubmitResponseDTO submitRun2Dot1(RunsSubmitRequest2Dot1DTO runsSubmitRequestDTO) throws HttpException {
        String pathSuffix = "api/2.1/jobs" + "/runs/submit";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(runsSubmitRequestDTO));

        checkResponse(response);
        return response.readEntity(RunsSubmitResponseDTO.class);
    }

    public RunsDTO listRuns() throws HttpException  {
        String pathSuffix = ENDPOINT_TARGET + "/runs/list";

        Response response = Session.getRequestBuilder(pathSuffix).get();

        checkResponse(response);
        return response.readEntity(RunsDTO.class);
    }

    public RunDTO getRun(long runId) throws HttpException {
        //TODO should be DEBUG logging statement
        //System.out.println("getRun HTTP request for id "+runId);
        String pathSuffix = ENDPOINT_TARGET + "/runs/get";

        Response response = Session.getRequestBuilder(pathSuffix, "run_id", runId).get();

        checkResponse(response);
        return response.readEntity(RunDTO.class);
    }

    public void cancelRun(RunDTO runDTO) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/runs/cancel";

        Response response = Session.getRequestBuilder(pathSuffix).post(Entity.json(runDTO));
    }

    public JobRunOutputDTO getRunOutput(long runId) throws HttpException {
        String pathSuffix = ENDPOINT_TARGET + "/runs/get-output";

        Response response = Session.getRequestBuilder(pathSuffix, "run_id", runId).get();

        checkResponse(response);
        return response.readEntity(JobRunOutputDTO.class);
    }

}
