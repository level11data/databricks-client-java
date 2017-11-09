package com.level11data.databricks.job.builder;

import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import org.quartz.Trigger;
import java.util.TimeZone;

public abstract class InteractiveJobBuilder extends JobBuilder {
    public final InteractiveCluster Cluster;

    public InteractiveJobBuilder(InteractiveCluster cluster) {
        super();
        Cluster = cluster;
    }

    @Override
    protected InteractiveJobBuilder withName(String name) {
        return (InteractiveJobBuilder)super.withName(name);
    }

    @Override
    protected InteractiveJobBuilder withEmailNotificationOnStart(String email) {
        return (InteractiveJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    protected InteractiveJobBuilder withEmailNotificationOnSuccess(String email) {
        return (InteractiveJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    protected InteractiveJobBuilder withEmailNotificationOnFailure(String email) {
        return (InteractiveJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    protected InteractiveJobBuilder withTimeout(int seconds) {
        return (InteractiveJobBuilder)super.withTimeout(seconds);
    }

    @Override
    protected InteractiveJobBuilder withMaxRetries(int retries) {
        return (InteractiveJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    protected InteractiveJobBuilder withMinRetryInterval(int milliseconds) {
        return (InteractiveJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    protected InteractiveJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (InteractiveJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    protected InteractiveJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (InteractiveJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    protected InteractiveJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (InteractiveJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);
        jobSettingsDTO.ExistingClusterId = Cluster.Id;
        return jobSettingsDTO;
    }

}
