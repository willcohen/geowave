package mil.nga.giat.geowave.service.grpc;

import java.io.IOException;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.Feature;
import mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.VectorQueryGrpc;

public class GeoWaveGrpcServer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcServer.class.getName());

	private final int port;
	private final Server server;

	public static void main(
			String[] args )
			throws InterruptedException {
		System.out.println("starting server");
		GeoWaveGrpcServer server = null;
		try {
			server = new GeoWaveGrpcServer(
					8980);
			server.start();
			server.blockUntilShutdown();
		}
		catch (final Exception e) {
			LOGGER.error(e.getMessage());
		}
	}

	public GeoWaveGrpcServer(
			int port )
			throws IOException {
		this.port = port;
		server = ServerBuilder.forPort(
				port).addService(
				new GeoWaveGrpcVectorQueryService()).build();
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
						// Use stderr here since the logger may have been reset
						// by its JVM shutdown hook.
						System.err.println("*** shutting down gRPC server since JVM is shutting down");
						GeoWaveGrpcServer.this.stop();
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

			final String cql = request.getCql();
			final String storeName = request.getBaseParams().getStoreName();
			final StoreLoader storeLoader = new StoreLoader(
					storeName);

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
			GeotoolsFeatureDataAdapter adapter = null;

			// get a handle to the data store and also the adapter store
			dataStore = storeLoader.createDataStore();
			adapterStore = storeLoader.createAdapterStore();

			if (adapterId != null) {
				adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(adapterId);
			}

			try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
					CQLQuery.createOptimalQuery(
							cql,
							adapter,
							null))) {

				while (iterator.hasNext()) {
					SimpleFeature simpleFeature = iterator.next();
					SimpleFeatureType type = simpleFeature.getType();
					Feature.Builder b = Feature.newBuilder();
					for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
						b.putAttributes(
								type.getAttributeDescriptors().get(
										i).getLocalName(),
								simpleFeature.getAttribute(i) == null ? "" : simpleFeature.getAttribute(
										i).toString());
					}
					Feature f = b.build();
					responseObserver.onNext(f);
				}
				responseObserver.onCompleted();
			}
			catch (final Exception e) {
				LOGGER.error(e.getMessage());
			}
		}

		public void spatialQuery(
				SpatialQueryParameters request,
				StreamObserver<Feature> responseObserver ) {

			final String storeName = request.getBaseParams().getStoreName();
			final StoreLoader storeLoader = new StoreLoader(
					storeName);

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

			// get a handle to the data store and also the adapter store
			dataStore = storeLoader.createDataStore();

			final String geomDefinition = request.getGeometry();
			Geometry queryGeom = null;
			try {
				queryGeom = new WKTReader(
						JTSFactoryFinder.getGeometryFactory()).read(geomDefinition);
			}
			catch (final Exception e) {
				LOGGER.error(e.getMessage());
			}

			final QueryOptions options = new QueryOptions(
					adapterId,
					indexId);

			try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
					options,
					new SpatialQuery(
							queryGeom))) {
				while (iterator.hasNext()) {
					SimpleFeature simpleFeature = iterator.next();
					SimpleFeatureType type = simpleFeature.getType();
					Feature.Builder b = Feature.newBuilder();
					for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
						b.putAttributes(
								type.getAttributeDescriptors().get(
										i).getLocalName(),
								simpleFeature.getAttribute(i) == null ? "" : simpleFeature.getAttribute(
										i).toString());
					}
					Feature f = b.build();
					responseObserver.onNext(f);
				}
				responseObserver.onCompleted();
			}
			catch (final Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
	}
}
