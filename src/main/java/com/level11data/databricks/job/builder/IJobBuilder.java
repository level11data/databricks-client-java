package com.level11data.databricks.job.builder;

import com.level11data.databricks.job.IJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.ILibrary;
import org.quartz.Trigger;

import java.io.File;
import java.net.URI;
import java.util.TimeZone;

public interface IJobBuilder {
    IJobBuilder withName(String name);

    IJobBuilder withEmailNotificationOnStart(String email);

    IJobBuilder withEmailNotificationOnSuccess(String email);

    IJobBuilder withEmailNotificationOnFailure(String email);

    IJobBuilder withTimeout(int seconds);

    IJobBuilder withMaxRetries(int retries);

    IJobBuilder withMinRetryInterval(int milliseconds);

    IJobBuilder withRetryOnTimeout(boolean retryOnTimeout);

    IJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns);

    IJobBuilder withSchedule(Trigger trigger, TimeZone timeZone);

    IJob create() throws JobConfigException;

    //TODO change this to return an Interface IClusterBuilder
    //This is only for AutomatedJobBuilder subclasses
    //AutomatedClusterBuilder withClusterSpec(int numWorkers);

    IJobBuilder withLibrary(ILibrary library) throws JobConfigException;

    IJobBuilder withJarLibrary(URI uri) throws JobConfigException;

    IJobBuilder withJarLibrary(URI uri, File libraryFile) throws JobConfigException;

    IJobBuilder withEggLibrary(URI uri) throws JobConfigException;

    IJobBuilder withEggLibrary(URI uri, File libraryFile) throws JobConfigException;

    IJobBuilder withMavenLibrary(String coordinates) throws JobConfigException;

    IJobBuilder withMavenLibrary(String coordinates, String repo) throws JobConfigException;

    IJobBuilder withMavenLibrary(String coordinates, String repo, String[] exclusions) throws JobConfigException;

    IJobBuilder withMavenLibrary(String coordinates, String[] exclusions) throws JobConfigException;

    IJobBuilder withPyPiLibrary(String packageName) throws JobConfigException;

    IJobBuilder withPyPiLibrary(String packageName, String repo) throws JobConfigException;

    IJobBuilder withCranLibrary(String packageName) throws JobConfigException;

    IJobBuilder withCranLibrary(String packageName, String repo) throws JobConfigException;

}
