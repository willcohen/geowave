/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteStreamHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;

public class BigtableEmulator
{
	private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BigtableEmulator.class);

	// Property names
	public static final String HOST_PORT_PROPERTY = "bigtable.emulator.endpoint";
	public static final String INTERNAL_PROPERTY = "bigtable.emulator.internal";
	public static final String DOWNLOAD_URL_PROPERTY = "bigtable.sdk.url";
	public static final String DOWNLOAD_FILE_PROPERTY = "bigtable.sdk.file";

	// Download and executable paths
	private String downloadUrl;
	private String fileName;

	private final static String GCLOUD_EXE_DIR = "google-cloud-sdk/bin";

	private static final long EMULATOR_SPINUP_DELAY_MS = 60000L;

	private final File sdkDir;
	private ExecuteWatchdog watchdog;

	public BigtableEmulator(
			String sdkDir,
			String sdkDownloadUrl,
			String sdkFileName ) {
		if (TestUtils.isSet(sdkDir)) {
			this.sdkDir = new File(
					sdkDir);
		}
		else {
			this.sdkDir = new File(
					TestUtils.TEMP_DIR,
					"gcloud");
		}

		this.downloadUrl = sdkDownloadUrl;
		this.fileName = sdkFileName;

		if (!this.sdkDir.exists() && !this.sdkDir.mkdirs()) {
			LOGGER.warn("unable to create directory " + this.sdkDir.getAbsolutePath());
		}
	}

	public boolean start(
			final String emulatorHostPort ) {
		if (!isInstalled()) {
			try {
				if (!install()) {
					return false;
				}
			}
			catch (IOException e) {
				LOGGER.error(e.getMessage());
				return false;
			}
		}

		try {
			startEmulator(emulatorHostPort);
		}
		catch (IOException | InterruptedException e) {
			LOGGER.error(e.getMessage());
			return false;
		}

		return true;
	}

	public boolean isRunning() {
		return (watchdog != null && watchdog.isWatching());
	}

	public void stop() {
		// first, ask the watchdog nicely:
		watchdog.destroyProcess();

		// then kill all the extra emulator processes like this:
		final String KILL_CMD_1 = "for i in $(ps -ef | grep -i \"[b]eta emulators bigtable\" | awk '{print $2}'); do kill -9 $i; done";
		final String KILL_CMD_2 = "for i in $(ps -ef | grep -i \"[c]btemulator\" | awk '{print $2}'); do kill -9 $i; done";

		File bashFile = new File(
				TestUtils.TEMP_DIR,
				"kill-bigtable.sh");

		PrintWriter scriptWriter;
		try {
			Writer w = new OutputStreamWriter(
					new FileOutputStream(
							bashFile),
					"UTF-8");
			scriptWriter = new PrintWriter(
					w);
			scriptWriter.println("#!/bin/bash");
			scriptWriter.println("set -ev");
			scriptWriter.println(KILL_CMD_1);
			scriptWriter.println(KILL_CMD_2);
			scriptWriter.close();

			bashFile.setExecutable(true);
		}
		catch (FileNotFoundException e1) {
			LOGGER.error(
					"Unable to create bigtable emulator kill script",
					e1);
			return;
		}
		catch (UnsupportedEncodingException e) {
			LOGGER.error(
					"Unable to create bigtable emulator kill script",
					e);
		}

		CommandLine cmdLine = new CommandLine(
				bashFile.getAbsolutePath());
		DefaultExecutor executor = new DefaultExecutor();
		int exitValue = 0;

		try {
			exitValue = executor.execute(cmdLine);
		}
		catch (IOException ex) {
			LOGGER.error(
					"Unable to execute bigtable emulator kill script",
					ex);
		}

		LOGGER.warn("Bigtable emulator " + (exitValue == 0 ? "stopped" : "failed to stop"));
	}

	private boolean isInstalled() {
		final File gcloudExe = new File(
				sdkDir,
				GCLOUD_EXE_DIR + "/gcloud");

		return (gcloudExe.canExecute());
	}

	protected boolean install()
			throws IOException {
		URL url = new URL(
				downloadUrl + "/" + fileName);

		final File downloadFile = new File(
				sdkDir.getParentFile(),
				fileName);
		if (!downloadFile.exists()) {
			try (FileOutputStream fos = new FileOutputStream(
					downloadFile)) {
				IOUtils.copyLarge(
						url.openStream(),
						fos);
				fos.flush();
			}
		}
		if (downloadFile.getName().endsWith(
				".zip")) {
			ZipUtils.unZipFile(
					downloadFile,
					sdkDir.getAbsolutePath());
		}
		else if (downloadFile.getName().endsWith(
				".tar.gz")) {
			final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
			unarchiver.enableLogging(new ConsoleLogger(
					Logger.LEVEL_WARN,
					"Gcloud SDK Unarchive"));
			unarchiver.setSourceFile(downloadFile);
			unarchiver.setDestDirectory(sdkDir);
			unarchiver.extract();
		}
		if (!downloadFile.delete()) {
			LOGGER.warn("cannot delete " + downloadFile.getAbsolutePath());
		}
		// Check the install
		if (!isInstalled()) {
			LOGGER.error("Gcloud install failed");
			return false;
		}

		// Install the beta components
		final File gcloudExe = new File(
				sdkDir,
				GCLOUD_EXE_DIR + "/gcloud");

		CommandLine cmdLine = new CommandLine(
				gcloudExe);
		cmdLine.addArgument("components");
		cmdLine.addArgument("install");
		cmdLine.addArgument("beta");
		cmdLine.addArgument("--quiet");
		DefaultExecutor executor = new DefaultExecutor();
		int exitValue = executor.execute(cmdLine);

		return (exitValue == 0);
	}

	/**
	 * Using apache commons exec for cmd line execution
	 * 
	 * @param command
	 * @return exitCode
	 * @throws ExecuteException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void startEmulator(
			final String emulatorHostPort )
			throws ExecuteException,
			IOException,
			InterruptedException {
		CommandLine cmdLine = new CommandLine(
				sdkDir + "/" + GCLOUD_EXE_DIR + "/gcloud");
		cmdLine.addArgument("beta");
		cmdLine.addArgument("emulators");
		cmdLine.addArgument("bigtable");
		cmdLine.addArgument("start");
		cmdLine.addArgument("--quiet");
		cmdLine.addArgument("--host-port");
		cmdLine.addArgument(emulatorHostPort);

		// Using a result handler makes the emulator run async
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

		// watchdog shuts down the emulator, later
		watchdog = new ExecuteWatchdog(
				ExecuteWatchdog.INFINITE_TIMEOUT);
		Executor executor = new DefaultExecutor();
		executor.setWatchdog(watchdog);
		executor.setStreamHandler(new ExecuteStreamHandler() {
			
			@Override
			public void stop()
					throws IOException {
			}
			
			@Override
			public void start()
					throws IOException {
			}
			
			@Override
			public void setProcessOutputStream(
					InputStream is )
					throws IOException {
			}
			
			@Override
			public void setProcessInputStream(
					OutputStream os )
					throws IOException {
			}
			
			@Override
			public void setProcessErrorStream(
					InputStream is )
					throws IOException {	
			}
		});

		LOGGER.warn("Starting BigTable Emulator: " + cmdLine.toString());

		executor.execute(
				cmdLine,
				resultHandler);

		// we need to wait here for a bit, in case the emulator needs to update
		// itself
		Thread.sleep(EMULATOR_SPINUP_DELAY_MS);
	}
}
