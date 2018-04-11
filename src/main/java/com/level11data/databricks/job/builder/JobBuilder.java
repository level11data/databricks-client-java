package com.level11data.databricks.job.builder;

import com.level11data.databricks.job.Job;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.Library;
import org.quartz.Trigger;

import java.io.File;
import java.net.URI;
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

    JobBuilder withLibrary(Library library) throws JobConfigException;

    JobBuilder withJarLibrary(URI uri) throws JobConfigException;

    JobBuilder withJarLibrary(URI uri, File libraryFile) throws JobConfigException;

    JobBuilder withEggLibrary(URI uri) throws JobConfigException;

    JobBuilder withEggLibrary(URI uri, File libraryFile) throws JobConfigException;

    JobBuilder withMavenLibrary(String coordinates) throws JobConfigException;

    JobBuilder withMavenLibrary(String coordinates, String repo) throws JobConfigException;

    JobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) throws JobConfigException;

    JobBuilder withMavenLibrary(String coordinates, String[] exclusions) throws JobConfigException;

    JobBuilder withPyPiLibrary(String packageName) throws JobConfigException;

    JobBuilder withPyPiLibrary(String packageName, String repo) throws JobConfigException;

    JobBuilder withCranLibrary(String packageName) throws JobConfigException;

    JobBuilder withCranLibrary(String packageName, String repo) throws JobConfigException;

}
