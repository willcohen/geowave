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

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.junit.Assert;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.ingest.operations.LocalToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.cli.remote.ListStatsCommand;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class TestUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

	public static enum DimensionalityType {
		SPATIAL(
				"spatial"),
		SPATIAL_TEMPORAL(
				"spatial_temporal"),
		ALL(
				"spatial,spatial_temporal");
		private final String dimensionalityArg;

		private DimensionalityType(
				final String dimensionalityArg ) {
			this.dimensionalityArg = dimensionalityArg;
		}

		public String getDimensionalityArg() {
			return dimensionalityArg;
		}
	}

	public static final File TEMP_DIR = new File(
			"./target/temp");

	public static final String TEST_FILTER_START_TIME_ATTRIBUTE_NAME = "StartTime";
	public static final String TEST_FILTER_END_TIME_ATTRIBUTE_NAME = "EndTime";
	public static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test";
	public static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	public static final String TEST_CASE_BASE = "data/";

	public static final PrimaryIndex DEFAULT_SPATIAL_INDEX = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex();
	public static final PrimaryIndex DEFAULT_SPATIAL_TEMPORAL_INDEX = new SpatialTemporalDimensionalityTypeProvider()
			.createPrimaryIndex();

	public static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

	public static void testLocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String ingestFilePath,
			final int nthreads ) {
		testLocalIngest(
				dataStore,
				dimensionalityType,
				ingestFilePath,
				"geotools-vector",
				nthreads);

	}

	public static void testLocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String ingestFilePath ) {
		testLocalIngest(
				dataStore,
				dimensionalityType,
				ingestFilePath,
				"geotools-vector",
				1);
	}

	public static boolean isSet(
			final String str ) {
		return (str != null) && !str.isEmpty();
	}

	public static void deleteAll(
			final DataStorePluginOptions dataStore ) {
		dataStore.createDataStore().delete(
				new QueryOptions(),
				null);
	}

	public static void testLocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String ingestFilePath,
			final String format,
			final int nthreads ) {

		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments

		// Ingest Formats
		final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin(format);

		// Indexes
		final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(
				",");
		final List<IndexPluginOptions> indexOptions = new ArrayList<IndexPluginOptions>(
				indexTypes.length);
		for (final String indexType : indexTypes) {
			final IndexPluginOptions indexOption = new IndexPluginOptions();
			indexOption.selectPlugin(indexType);
			indexOptions.add(indexOption);
		}

		// Create the command and execute.
		final LocalToGeowaveCommand localIngester = new LocalToGeowaveCommand();
		localIngester.setPluginFormats(ingestFormatOptions);
		localIngester.setInputIndexOptions(indexOptions);
		localIngester.setInputStoreOptions(dataStore);
		localIngester.setParameters(
				ingestFilePath,
				null,
				null);
		localIngester.setThreads(nthreads);
		localIngester.execute(new ManualOperationParams());

		verifyStats(dataStore);

	}

	private static void verifyStats(
			final DataStorePluginOptions dataStore ) {
		final ListStatsCommand listStats = new ListStatsCommand();
		listStats.setInputStoreOptions(dataStore);
		listStats.setParameters(
				null,
				null);
		try {
			listStats.execute(new ManualOperationParams());
		}
		catch (final ParameterException e) {
			throw new RuntimeException(
					e);
		}
	}

	public static long hashCentroid(
			final Geometry geometry ) {
		final Point centroid = geometry.getCentroid();
		return Double.doubleToLongBits(centroid.getX()) + Double.doubleToLongBits(centroid.getY() * 31);
	}

	public static class ExpectedResults
	{
		public Set<Long> hashedCentroids;
		public int count;

		@SuppressFBWarnings({
			"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"
		})
		public ExpectedResults(
				final Set<Long> hashedCentroids,
				final int count ) {
			this.hashedCentroids = hashedCentroids;
			this.count = count;
		}
	}

	public static ExpectedResults getExpectedResults(
			final CloseableIterator<?> results )
			throws IOException {
		final Set<Long> hashedCentroids = new HashSet<Long>();
		int expectedResultCount = 0;
		try {
			while (results.hasNext()) {
				final Object obj = results.next();
				if (obj instanceof SimpleFeature) {
					expectedResultCount++;
					final SimpleFeature feature = (SimpleFeature) obj;
					hashedCentroids.add(hashCentroid((Geometry) feature.getDefaultGeometry()));
				}
			}
		}
		finally {
			results.close();
		}
		return new ExpectedResults(
				hashedCentroids,
				expectedResultCount);
	}

	public static ExpectedResults getExpectedResults(
			final URL[] expectedResultsResources )
			throws IOException {
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
		final Set<Long> hashedCentroids = new HashSet<Long>();
		int expectedResultCount = 0;
		for (final URL expectedResultsResource : expectedResultsResources) {
			map.put(
					"url",
					expectedResultsResource);
			SimpleFeatureIterator featureIterator = null;
			try {
				dataStore = DataStoreFinder.getDataStore(map);
				if (dataStore == null) {
					LOGGER.error("Could not get dataStore instance, getDataStore returned null");
					throw new IOException(
							"Could not get dataStore instance, getDataStore returned null");
				}
				final SimpleFeatureCollection expectedResults = dataStore.getFeatureSource(
						dataStore.getNames().get(
								0)).getFeatures();

				expectedResultCount += expectedResults.size();
				// unwrap the expected results into a set of features IDs so its
				// easy to check against
				featureIterator = expectedResults.features();
				while (featureIterator.hasNext()) {
					final SimpleFeature feature = featureIterator.next();
					final long centroid = hashCentroid((Geometry) feature.getDefaultGeometry());
					hashedCentroids.add(centroid);
				}
			}
			finally {
				IOUtils.closeQuietly(featureIterator);
				if (dataStore != null) {
					dataStore.dispose();
				}
			}
		}
		return new ExpectedResults(
				hashedCentroids,
				expectedResultCount);
	}

	public static DistributableQuery resourceToQuery(
			final URL filterResource )
			throws IOException {
		return featureToQuery(resourceToFeature(filterResource));
	}

	public static SimpleFeature resourceToFeature(
			final URL filterResource )
			throws IOException {
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
		map.put(
				"url",
				filterResource);
		final SimpleFeature savedFilter;
		SimpleFeatureIterator sfi = null;
		try {
			dataStore = DataStoreFinder.getDataStore(map);
			if (dataStore == null) {
				LOGGER.error("Could not get dataStore instance, getDataStore returned null");
				throw new IOException(
						"Could not get dataStore instance, getDataStore returned null");
			}
			// just grab the first feature and use it as a filter
			sfi = dataStore.getFeatureSource(
					dataStore.getNames().get(
							0)).getFeatures().features();
			savedFilter = sfi.next();

		}
		finally {
			if (sfi != null) {
				sfi.close();
			}
			if (dataStore != null) {
				dataStore.dispose();
			}
		}
		return savedFilter;
	}

	protected static DistributableQuery featureToQuery(
			final SimpleFeature savedFilter ) {
		final Geometry filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
		final Object startObj = savedFilter.getAttribute(TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
		final Object endObj = savedFilter.getAttribute(TEST_FILTER_END_TIME_ATTRIBUTE_NAME);

		if ((startObj != null) && (endObj != null)) {
			// if we can resolve start and end times, make it a spatial temporal
			// query
			Date startDate = null, endDate = null;
			if (startObj instanceof Calendar) {
				startDate = ((Calendar) startObj).getTime();
			}
			else if (startObj instanceof Date) {
				startDate = (Date) startObj;
			}
			if (endObj instanceof Calendar) {
				endDate = ((Calendar) endObj).getTime();
			}
			else if (endObj instanceof Date) {
				endDate = (Date) endObj;
			}
			if ((startDate != null) && (endDate != null)) {
				return new SpatialTemporalQuery(
						startDate,
						endDate,
						filterGeometry);
			}
		}
		// otherwise just return a spatial query
		return new SpatialQuery(
				filterGeometry);
	}

	static protected void replaceParameters(
			final Map<String, String> values,
			final File file )
			throws IOException {
		{
			String str = FileUtils.readFileToString(file);
			for (final Entry<String, String> entry : values.entrySet()) {
				str = str.replaceAll(
						entry.getKey(),
						entry.getValue());
			}
			FileUtils.deleteQuietly(file);
			FileUtils.write(
					file,
					str);
		}
	}

	/**
	 *
	 * @param bi
	 *            sample
	 * @param ref
	 *            reference
	 * @param minPctError
	 *            used for testing subsampling - to ensure we are properly
	 *            subsampling we want there to be some error if subsampling is
	 *            aggressive (10 pixels)
	 * @param maxPctError
	 *            used for testing subsampling - we want to ensure at most we
	 *            are off by this percentile
	 */
	public static void testTileAgainstReference(
			final BufferedImage actual,
			final BufferedImage expected,
			final double minPctError,
			final double maxPctError ) {
		Assert.assertEquals(
				expected.getWidth(),
				actual.getWidth());
		Assert.assertEquals(
				expected.getHeight(),
				actual.getHeight());
		final int totalPixels = expected.getWidth() * expected.getHeight();
		final int minErrorPixels = (int) Math.round(minPctError * totalPixels);
		final int maxErrorPixels = (int) Math.round(maxPctError * totalPixels);
		int errorPixels = 0;
		// test under default style
		for (int x = 0; x < expected.getWidth(); x++) {
			for (int y = 0; y < expected.getHeight(); y++) {
				if (actual.getRGB(
						x,
						y) != expected.getRGB(
						x,
						y)) {
					errorPixels++;
					if (errorPixels > maxErrorPixels) {
						Assert.fail(String.format(
								"[%d,%d] failed to match ref=%d gen=%d",
								x,
								y,
								expected.getRGB(
										x,
										y),
								actual.getRGB(
										x,
										y)));
					}
				}
			}
		}
		if (errorPixels < minErrorPixels) {
			Assert
					.fail(String
							.format(
									"Subsampling did not work as expected; error pixels (%d) did not exceed the minimum threshold (%d)",
									errorPixels,
									minErrorPixels));
		}

		if (errorPixels > 0) {
			System.out.println(((float) errorPixels / (float) totalPixels) + "% pixels differed from expected");
		}
	}

	public static double getTileValue(
			final int x,
			final int y,
			final int b,
			final int tileSize ) {
		// just use an arbitrary 'r'
		return getTileValue(
				x,
				y,
				b,
				3,
				tileSize);
	}

	public static void fillTestRasters(
			final WritableRaster raster1,
			final WritableRaster raster2,
			final int tileSize ) {
		// for raster1 do the following:
		// set every even row in bands 0 and 1
		// set every value incorrectly in band 2
		// set no values in band 3 and set every value in 4

		// for raster2 do the following:
		// set no value in band 0 and 4
		// set every odd row in band 1
		// set every value in bands 2 and 3

		// for band 5, set the lower 2x2 samples for raster 1 and the rest for
		// raster 2
		// for band 6, set the upper quadrant samples for raster 1 and the rest
		// for raster 2
		// for band 7, set the lower 2x2 samples to the wrong value for raster 1
		// and the expected value for raster 2 and set everything but the upper
		// quadrant for raster 2
		for (int x = 0; x < tileSize; x++) {
			for (int y = 0; y < tileSize; y++) {

				// just use x and y to arbitrarily end up with some wrong value
				// that can be ingested
				final double wrongValue = (getTileValue(
						y,
						x,
						y,
						tileSize) * 3) + 1;
				if ((x < 2) && (y < 2)) {
					raster1.setSample(
							x,
							y,
							5,
							getTileValue(
									x,
									y,
									5,
									tileSize));
					raster1.setSample(
							x,
							y,
							7,
							wrongValue);
					raster2.setSample(
							x,
							y,
							7,
							getTileValue(
									x,
									y,
									7,
									tileSize));
				}
				else {
					raster2.setSample(
							x,
							y,
							5,
							getTileValue(
									x,
									y,
									5,
									tileSize));
				}
				if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
					raster1.setSample(
							x,
							y,
							6,
							getTileValue(
									x,
									y,
									6,
									tileSize));
				}
				else {
					raster2.setSample(
							x,
							y,
							6,
							getTileValue(
									x,
									y,
									6,
									tileSize));
					raster2.setSample(
							x,
							y,
							7,
							getTileValue(
									x,
									y,
									7,
									tileSize));
				}
				if ((y % 2) == 0) {
					raster1.setSample(
							x,
							y,
							0,
							getTileValue(
									x,
									y,
									0,
									tileSize));
					raster1.setSample(
							x,
							y,
							1,
							getTileValue(
									x,
									y,
									1,
									tileSize));
				}
				raster1.setSample(
						x,
						y,
						2,
						wrongValue);

				raster1.setSample(
						x,
						y,
						4,
						getTileValue(
								x,
								y,
								4,
								tileSize));
				if ((y % 2) != 0) {
					raster2.setSample(
							x,
							y,
							1,
							getTileValue(
									x,
									y,
									1,
									tileSize));
				}
				raster2.setSample(
						x,
						y,
						2,
						TestUtils.getTileValue(
								x,
								y,
								2,
								tileSize));

				raster2.setSample(
						x,
						y,
						3,
						getTileValue(
								x,
								y,
								3,
								tileSize));
			}
		}
	}

	private static Random rng = null;

	public static double getTileValue(
			final int x,
			final int y,
			final int b,
			final int r,
			final int tileSize ) {
		// make this some random but repeatable and vary the scale
		final double resultOfFunction = randomFunction(
				x,
				y,
				b,
				r,
				tileSize);
		// this is meant to just vary the scale
		if ((r % 2) == 0) {
			return resultOfFunction;
		}
		else {
			if (rng == null) {
				rng = new Random(
						(long) resultOfFunction);
			}
			else {
				rng.setSeed((long) resultOfFunction);
			}

			return rng.nextDouble() * resultOfFunction;
		}
	}

	private static double randomFunction(
			final int x,
			final int y,
			final int b,
			final int r,
			final int tileSize ) {
		return (((x + (y * tileSize)) * .1) / (b + 1)) + r;
	}
}
