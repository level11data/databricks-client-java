package com.level11data.databricks.job.builder;

import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.library.Library;

import java.io.File;
import java.net.URI;

public interface JobBuilderWithLibraries extends JobBuilder {

    JobBuilderWithLibraries withLibrary(Library library) throws JobConfigException;

    JobBuilderWithLibraries withJarLibrary(URI uri) throws JobConfigException;

    JobBuilderWithLibraries withJarLibrary(URI uri, File libraryFile) throws JobConfigException;

    JobBuilderWithLibraries withEggLibrary(URI uri) throws JobConfigException;

    JobBuilderWithLibraries withEggLibrary(URI uri, File libraryFile) throws JobConfigException;

    JobBuilderWithLibraries withMavenLibrary(String coordinates) throws JobConfigException;

    JobBuilderWithLibraries withMavenLibrary(String coordinates, String repo) throws JobConfigException;

    JobBuilderWithLibraries withMavenLibrary(String coordinates, String repo, String[] exclusions) throws JobConfigException;

    JobBuilderWithLibraries withMavenLibrary(String coordinates, String[] exclusions) throws JobConfigException;

    JobBuilderWithLibraries withPyPiLibrary(String packageName) throws JobConfigException;

    JobBuilderWithLibraries withPyPiLibrary(String packageName, String repo) throws JobConfigException;

    JobBuilderWithLibraries withCranLibrary(String packageName) throws JobConfigException;

    JobBuilderWithLibraries withCranLibrary(String packageName, String repo) throws JobConfigException;

}
