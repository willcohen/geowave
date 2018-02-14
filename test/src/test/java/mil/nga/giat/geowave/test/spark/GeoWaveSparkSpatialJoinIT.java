package mil.nga.giat.geowave.test.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.analytic.spark.sparksql.SimpleFeatureDataFrame;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunctionRegistry;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomIntersects;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import mil.nga.giat.geowave.analytic.spark.spatial.SpatialJoin;
import mil.nga.giat.geowave.analytic.spark.spatial.TieredSpatialJoin;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveSparkSpatialJoinIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSpatialJoinIT.class);
	
	@GeoWaveTestStore(value = {
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;
	private static SparkSession session;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		session = SparkSession
				.builder()
				.appName("SpatialJoinTest")
				.master("local[*]")
				.config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
				.config("spark.kryo.registrator", "mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator")
				.getOrCreate();

		GeomFunctionRegistry.registerGeometryFunctions(session);
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveSparkSpatialJoinIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
		
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveSparkSpatialJoinIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testHailTornadoIntersection() {
		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());
		long mark = System.currentTimeMillis();
		
		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + 1 + " thread(s).");

		mark = System.currentTimeMillis();

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + 1 + " thread(s).");
		


		SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
		PrimaryIndex index = provider.createPrimaryIndex();
		TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();
		
		TieredSpatialJoin tieredJoin = new TieredSpatialJoin();
		ByteArrayId hail_adapter = new ByteArrayId("hail");
		ByteArrayId tornado_adapter = new ByteArrayId("tornado_tracks");
		GeomIntersects intersectsPredicate = new GeomIntersects();
		
		DataAdapter<?> hailAdapter = dataStore.createAdapterStore().getAdapter(hail_adapter);
		DataAdapter<?> tornadoAdapter = dataStore.createAdapterStore().getAdapter(tornado_adapter);

		JavaPairRDD<GeoWaveInputKey, SimpleFeature> hailRDD = null;
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> tornadoRDD = null;
		try {
			hailRDD = GeoWaveRDD.rddForSimpleFeatures(
					session.sparkContext(), 
					dataStore, 
					null, 
					new QueryOptions(hailAdapter)).reduceByKey((f1,f2) -> f1);

			tornadoRDD = GeoWaveRDD.rddForSimpleFeatures(
					session.sparkContext(), 
					dataStore, 
					null, 
					new QueryOptions(tornadoAdapter)).reduceByKey((f1,f2) -> f1);
		}
		catch (IOException e) {
			LOGGER.error("Could not perform join");
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			session.close();
			Assert.fail();
		}
		
		long tornadoIndexedCount = 0;
		long hailIndexedCount = 0;

		LOGGER.warn("------------ Running indexed spatial join. ----------");
		mark = System.currentTimeMillis();
		tieredJoin.join(session, hailRDD, tornadoRDD, intersectsPredicate, strategy);
		hailIndexedCount = tieredJoin.leftJoined.count();
		tornadoIndexedCount = tieredJoin.rightJoined.count();
		long indexJoinDur = (System.currentTimeMillis() - mark);

		
		long tornadoBruteCount = 0;
		long hailBruteCount = 0;
		Dataset<Row> hailBruteResults = null;
		Dataset<Row> tornadoBruteResults = null;
		
		LOGGER.warn("------------ Running Brute force spatial join. ----------");
		mark = System.currentTimeMillis();
		SimpleFeatureDataFrame hailFrame = new SimpleFeatureDataFrame(session);
		SimpleFeatureDataFrame tornadoFrame = new SimpleFeatureDataFrame(session);

		tornadoFrame.init(dataStore, tornado_adapter);
		tornadoFrame.getDataFrame(tornadoRDD).createOrReplaceTempView("tornado");
		

		hailFrame.init(dataStore, hail_adapter);
		hailFrame.getDataFrame(hailRDD).createOrReplaceTempView("hail");
		
		hailBruteResults = session.sql("select hail.* from hail, tornado where geomIntersects(hail.geom,tornado.geom)");
		hailBruteResults = hailBruteResults.dropDuplicates();
		hailBruteCount = hailBruteResults.count();
		
		tornadoBruteResults = session.sql("select tornado.* from hail, tornado where geomIntersects(hail.geom,tornado.geom)");
		tornadoBruteResults = tornadoBruteResults.dropDuplicates();
		tornadoBruteCount = tornadoBruteResults.count();
		dur = (System.currentTimeMillis() - mark);
		
		LOGGER.warn("Indexed tornado join count= " + tornadoIndexedCount );
		LOGGER.warn("Indexed hail join count= " + hailIndexedCount );
		LOGGER.warn("Indexed join duration = " + indexJoinDur + " ms.");
		
		LOGGER.warn("Brute tornado join count= " + tornadoBruteCount);
		LOGGER.warn("Brute hail join count= " + hailBruteCount);
		LOGGER.warn("Brute join duration = " + dur + " ms.");
		

	
		TestUtils.deleteAll(dataStore);
		session.close();
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}