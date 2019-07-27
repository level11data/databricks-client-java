package com.level11data.databricks.job.builder;

import com.level11data.databricks.job.Job;
import com.level11data.databricks.job.JobConfigException;
import org.quartz.Trigger;
import java.util.TimeZone;

public interface JobBuilder {

    JobBuilder withName(String name);

    JobBuilder withEmailNotificationOnStart(String email);

    JobBuilder withEmailNotificationOnSuccess(String email);

    JobBuilder withEmailNotificationOnFailure(String email);

    JobBuilder withTimeout(int seconds);

    JobBuilder withMaxRetries(int retries);

    JobBuilder withMinRetryInterval(int milliseconds);

    JobBuilder withRetryOnTimeout(boolean retryOnTimeout);

    JobBuilder withMaxConcurrentRuns(int maxConcurrentRuns);

    JobBuilder withSchedule(Trigger trigger, TimeZone timeZone);

    Job create() throws JobConfigException;
}
