package mil.nga.giat.geowave.service.grpc;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.factory.FactoryRegistryException;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;

import com.beust.jcommander.ParameterException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.CQLQueryParameters;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.Coordinate;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.Feature;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.SpatialQueryParameters;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.VectorQueryGrpc;

public class GeoWaveGrpcQueryServer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcQueryServer.class.getName());

	private final int port;
	private final Server server;

	public GeoWaveGrpcQueryServer(
			int port )
			throws IOException {
		this.port = port;
		server = ServerBuilder.forPort(
				port).addService(
				new GeoWaveGrpcVectorQueryService()).build();
	}

	public static void main(
			String[] args )
			throws Exception {
		GeoWaveGrpcQueryServer server = new GeoWaveGrpcQueryServer(
				8980);
		server.start();
		server.blockUntilShutdown();
	}

	/** Start serving requests. */
	public void start()
			throws IOException {
		server.start();
		LOGGER.info("Server started, listening on " + port);
		Runtime.getRuntime().addShutdownHook(
				new Thread() {
					@Override
					public void run() {
						// Use stderr here since the logger may has been reset
						// by its JVM shutdown hook.
						System.err.println("*** shutting down gRPC server since JVM is shutting down");
						GeoWaveGrpcQueryServer.this.stop();
						System.err.println("*** server shut down");
					}
				});
	}

	/** Stop serving requests and shutdown resources. */
	public void stop() {
		if (server != null) {
			server.shutdown();
		}
	}

	/**
	 * Await termination on the main thread since the grpc library uses daemon
	 * threads.
	 */
	private void blockUntilShutdown()
			throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

	private static class GeoWaveGrpcVectorQueryService extends
			VectorQueryGrpc.VectorQueryImplBase
	{
		@Override
		public void cqlQuery(
				CQLQueryParameters request,
				StreamObserver<Feature> responseObserver ) {

			final String storeName = request.getBaseParams().getStoreName();
			final StoreLoader storeLoader = new StoreLoader(
					request.getBaseParams().getStoreName());

			final ByteArrayId adapterId = new ByteArrayId(
					request.getBaseParams().getAdapterId().toByteArray());
			final ByteArrayId indexId = new ByteArrayId(
					request.getBaseParams().getIndexId().toByteArray());

			// first check to make sure the data store exists
			if (!storeLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
				throw new ParameterException(
						"Cannot find store name: " + storeLoader.getStoreName());
			}

			DataStore dataStore = null;
			AdapterStore adapterStore = null;
			IndexStore indexStore = null;
			GeotoolsFeatureDataAdapter adapter = null;
			try {
				// get a handle to the data store and also the adapter store
				dataStore = storeLoader.createDataStore();
				adapterStore = storeLoader.createAdapterStore();
				indexStore = storeLoader.createIndexStore();

				if (adapterId != null) {
					adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(adapterId);
				}
				else {
					final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
					adapter = (GeotoolsFeatureDataAdapter) it.next();
					it.close();
				}
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to read adapter",
						e);
			}

			try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
					CQLQuery.createOptimalQuery(
							request.getCql(),
							adapter,
							null,
							null))) {

				while (iterator.hasNext()) {
					System.out.println("Query match: " + iterator.next().getID());
				}
			}
			catch (CQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// Hardcoded feature to return for testing purposes
			/*
			 * Feature f = Feature.newBuilder().putAttributes( "Geometry",
			 * "0,0").putAttributes( "Name", "Point").build();
			 * responseObserver.onNext(f); responseObserver.onCompleted();
			 */
		}

		public void spatialQuery(
				SpatialQueryParameters request,
				StreamObserver<Feature> responseObserver ) {
			final String storeName = request.getBaseParams().getStoreName();
			final StoreLoader storeLoader = new StoreLoader(
					request.getBaseParams().getStoreName());

			final ByteArrayId adapterId = new ByteArrayId(
					request.getBaseParams().getAdapterId().toByteArray());
			final ByteArrayId indexId = new ByteArrayId(
					request.getBaseParams().getIndexId().toByteArray());

			// first check to make sure the data store exists
			if (!storeLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
				throw new ParameterException(
						"Cannot find store name: " + storeLoader.getStoreName());
			}

			DataStore dataStore = null;
			AdapterStore adapterStore = null;
			IndexStore indexStore = null;
			GeotoolsFeatureDataAdapter adapter = null;
			try {
				// get a handle to the data store and also the adapter store
				dataStore = storeLoader.createDataStore();
				adapterStore = storeLoader.createAdapterStore();
				indexStore = storeLoader.createIndexStore();

				if (adapterId != null) {
					adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(adapterId);
				}
				else {
					final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
					adapter = (GeotoolsFeatureDataAdapter) it.next();
					it.close();
				}
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to read adapter",
						e);
			}

			final String geomDefinition = request.getGeometry();

			Geometry queryGeom = null;
			try {
				queryGeom = new WKTReader(
						JTSFactoryFinder.getGeometryFactory()).read(geomDefinition);
			}
			catch (FactoryRegistryException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			final QueryOptions options = new QueryOptions(
					adapter,
					new SpatialIndexBuilder().createIndex());

			try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
					options,
					new SpatialQuery(
							queryGeom))) {

				while (iterator.hasNext()) {
					SimpleFeature sf = iterator.next();

					// log.info("Obtained SimpleFeature " +
					// sf.getName().toString() + " - " +
					// sf.getAttribute("filter"));
					// count++;
					System.out.println("Query match: " + sf.getID());
				}
				// log.info("Should have obtained 1 feature. -> " + (count ==
				// 1));
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
