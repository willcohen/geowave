package mil.nga.giat.geowave.service.grpc;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.io.IOException;
import java.util.logging.Logger;

import com.google.protobuf.Descriptors.FieldDescriptor;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.CQLQueryParameters;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.Coordinate;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.Feature;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.SpatialQueryParameters;
import mil.nga.giat.geowave.grpc.vector.query.protobuf.VectorQueryGrpc;

public class GeoWaveGrpcQueryServer
{
	private static final Logger logger = Logger.getLogger(GeoWaveGrpcQueryServer.class.getName());

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

	/** Start serving requests. */
	public void start()
			throws IOException {
		server.start();
		logger.info("Server started, listening on " + port);
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
			// Coordinate c = Coordinate.newBuilder().set
			Feature f = Feature.newBuilder().putAttributes("Geometry", "0,0").putAttributes("Name", "Point").build();
			responseObserver.onNext(f);
			responseObserver.onCompleted();
		}

		public void spatialQuery(
				SpatialQueryParameters request,
				StreamObserver<Feature> responseObserver ) {
			responseObserver.onCompleted();
		}
	}
}
