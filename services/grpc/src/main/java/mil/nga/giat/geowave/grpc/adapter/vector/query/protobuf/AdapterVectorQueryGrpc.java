package mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * Interface exported by the server.
 * </pre>
 */
@javax.annotation.Generated(value = "by gRPC proto compiler (version 1.0.1)", comments = "Source: AdapterVectorQuery.proto")
public class AdapterVectorQueryGrpc
{

	private AdapterVectorQueryGrpc() {}

	public static final String SERVICE_NAME = "AdapterVectorQuery";

	// Static method descriptors that strictly reflect the proto.
	@io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
	public static final io.grpc.MethodDescriptor<mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.CQLQueryParameters, mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.Feature> METHOD_CQL_QUERY = io.grpc.MethodDescriptor
			.create(
					io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
					generateFullMethodName(
							"AdapterVectorQuery",
							"CqlQuery"),
					io.grpc.protobuf.ProtoUtils
							.marshaller(mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.CQLQueryParameters
									.getDefaultInstance()),
					io.grpc.protobuf.ProtoUtils
							.marshaller(mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.Feature
									.getDefaultInstance()));

	/**
	 * Creates a new async stub that supports all call types for the service
	 */
	public static AdapterVectorQueryStub newStub(
			io.grpc.Channel channel ) {
		return new AdapterVectorQueryStub(
				channel);
	}

	/**
	 * Creates a new blocking-style stub that supports unary and streaming
	 * output calls on the service
	 */
	public static AdapterVectorQueryBlockingStub newBlockingStub(
			io.grpc.Channel channel ) {
		return new AdapterVectorQueryBlockingStub(
				channel);
	}

	/**
	 * Creates a new ListenableFuture-style stub that supports unary and
	 * streaming output calls on the service
	 */
	public static AdapterVectorQueryFutureStub newFutureStub(
			io.grpc.Channel channel ) {
		return new AdapterVectorQueryFutureStub(
				channel);
	}

	/**
	 * <pre>
	 * Interface exported by the server.
	 * </pre>
	 */
	public static abstract class AdapterVectorQueryImplBase implements
			io.grpc.BindableService
	{

		/**
		 * <pre>
		 * CQL query for vector-based data
		 * </pre>
		 */
		public void cqlQuery(
				mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.CQLQueryParameters request,
				io.grpc.stub.StreamObserver<mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.Feature> responseObserver ) {
			asyncUnimplementedUnaryCall(
					METHOD_CQL_QUERY,
					responseObserver);
		}

		@java.lang.Override
		public io.grpc.ServerServiceDefinition bindService() {
			return io.grpc.ServerServiceDefinition
					.builder(
							getServiceDescriptor())
					.addMethod(
							METHOD_CQL_QUERY,
							asyncServerStreamingCall(new MethodHandlers<mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.CQLQueryParameters, mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.Feature>(
									this,
									METHODID_CQL_QUERY)))
					.build();
		}
	}

	/**
	 * <pre>
	 * Interface exported by the server.
	 * </pre>
	 */
	public static final class AdapterVectorQueryStub extends
			io.grpc.stub.AbstractStub<AdapterVectorQueryStub>
	{
		private AdapterVectorQueryStub(
				io.grpc.Channel channel ) {
			super(
					channel);
		}

		private AdapterVectorQueryStub(
				io.grpc.Channel channel,
				io.grpc.CallOptions callOptions ) {
			super(
					channel,
					callOptions);
		}

		@java.lang.Override
		protected AdapterVectorQueryStub build(
				io.grpc.Channel channel,
				io.grpc.CallOptions callOptions ) {
			return new AdapterVectorQueryStub(
					channel,
					callOptions);
		}

		/**
		 * <pre>
		 * CQL query for vector-based data
		 * </pre>
		 */
		public void cqlQuery(
				mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.CQLQueryParameters request,
				io.grpc.stub.StreamObserver<mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.Feature> responseObserver ) {
			asyncServerStreamingCall(
					getChannel().newCall(
							METHOD_CQL_QUERY,
							getCallOptions()),
					request,
					responseObserver);
		}
	}

	/**
	 * <pre>
	 * Interface exported by the server.
	 * </pre>
	 */
	public static final class AdapterVectorQueryBlockingStub extends
			io.grpc.stub.AbstractStub<AdapterVectorQueryBlockingStub>
	{
		private AdapterVectorQueryBlockingStub(
				io.grpc.Channel channel ) {
			super(
					channel);
		}

		private AdapterVectorQueryBlockingStub(
				io.grpc.Channel channel,
				io.grpc.CallOptions callOptions ) {
			super(
					channel,
					callOptions);
		}

		@java.lang.Override
		protected AdapterVectorQueryBlockingStub build(
				io.grpc.Channel channel,
				io.grpc.CallOptions callOptions ) {
			return new AdapterVectorQueryBlockingStub(
					channel,
					callOptions);
		}

		/**
		 * <pre>
		 * CQL query for vector-based data
		 * </pre>
		 */
		public java.util.Iterator<mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.Feature> cqlQuery(
				mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.CQLQueryParameters request ) {
			return blockingServerStreamingCall(
					getChannel(),
					METHOD_CQL_QUERY,
					getCallOptions(),
					request);
		}
	}

	/**
	 * <pre>
	 * Interface exported by the server.
	 * </pre>
	 */
	public static final class AdapterVectorQueryFutureStub extends
			io.grpc.stub.AbstractStub<AdapterVectorQueryFutureStub>
	{
		private AdapterVectorQueryFutureStub(
				io.grpc.Channel channel ) {
			super(
					channel);
		}

		private AdapterVectorQueryFutureStub(
				io.grpc.Channel channel,
				io.grpc.CallOptions callOptions ) {
			super(
					channel,
					callOptions);
		}

		@java.lang.Override
		protected AdapterVectorQueryFutureStub build(
				io.grpc.Channel channel,
				io.grpc.CallOptions callOptions ) {
			return new AdapterVectorQueryFutureStub(
					channel,
					callOptions);
		}
	}

	private static final int METHODID_CQL_QUERY = 0;

	private static class MethodHandlers<Req, Resp> implements
			io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
			io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
			io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
			io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp>
	{
		private final AdapterVectorQueryImplBase serviceImpl;
		private final int methodId;

		public MethodHandlers(
				AdapterVectorQueryImplBase serviceImpl,
				int methodId ) {
			this.serviceImpl = serviceImpl;
			this.methodId = methodId;
		}

		@java.lang.Override
		@java.lang.SuppressWarnings("unchecked")
		public void invoke(
				Req request,
				io.grpc.stub.StreamObserver<Resp> responseObserver ) {
			switch (methodId) {
				case METHODID_CQL_QUERY:
					serviceImpl
							.cqlQuery(
									(mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.CQLQueryParameters) request,
									(io.grpc.stub.StreamObserver<mil.nga.giat.geowave.grpc.adapter.vector.query.protobuf.Feature>) responseObserver);
					break;
				default:
					throw new AssertionError();
			}
		}

		@java.lang.Override
		@java.lang.SuppressWarnings("unchecked")
		public io.grpc.stub.StreamObserver<Req> invoke(
				io.grpc.stub.StreamObserver<Resp> responseObserver ) {
			switch (methodId) {
				default:
					throw new AssertionError();
			}
		}
	}

	public static io.grpc.ServiceDescriptor getServiceDescriptor() {
		return new io.grpc.ServiceDescriptor(
				SERVICE_NAME,
				METHOD_CQL_QUERY);
	}

}
