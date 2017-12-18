package mil.nga.giat.geowave.service.grpc.protobuf;

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
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: GeoWaveVectorQuery.proto")
public class VectorQueryGrpc {

  private VectorQueryGrpc() {}

  public static final String SERVICE_NAME = "VectorQuery";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters,
      mil.nga.giat.geowave.service.grpc.protobuf.Feature> METHOD_CQL_QUERY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "VectorQuery", "CqlQuery"),
          io.grpc.protobuf.ProtoUtils.marshaller(mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mil.nga.giat.geowave.service.grpc.protobuf.Feature.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters,
      mil.nga.giat.geowave.service.grpc.protobuf.Feature> METHOD_SPATIAL_QUERY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "VectorQuery", "SpatialQuery"),
          io.grpc.protobuf.ProtoUtils.marshaller(mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mil.nga.giat.geowave.service.grpc.protobuf.Feature.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static VectorQueryStub newStub(io.grpc.Channel channel) {
    return new VectorQueryStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static VectorQueryBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new VectorQueryBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static VectorQueryFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new VectorQueryFutureStub(channel);
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static abstract class VectorQueryImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * CQL query for vector-based data
     * </pre>
     */
    public void cqlQuery(mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters request,
        io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.Feature> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CQL_QUERY, responseObserver);
    }

    /**
     */
    public void spatialQuery(mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters request,
        io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.Feature> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SPATIAL_QUERY, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_CQL_QUERY,
            asyncServerStreamingCall(
              new MethodHandlers<
                mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters,
                mil.nga.giat.geowave.service.grpc.protobuf.Feature>(
                  this, METHODID_CQL_QUERY)))
          .addMethod(
            METHOD_SPATIAL_QUERY,
            asyncServerStreamingCall(
              new MethodHandlers<
                mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters,
                mil.nga.giat.geowave.service.grpc.protobuf.Feature>(
                  this, METHODID_SPATIAL_QUERY)))
          .build();
    }
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class VectorQueryStub extends io.grpc.stub.AbstractStub<VectorQueryStub> {
    private VectorQueryStub(io.grpc.Channel channel) {
      super(channel);
    }

    private VectorQueryStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VectorQueryStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new VectorQueryStub(channel, callOptions);
    }

    /**
     * <pre>
     * CQL query for vector-based data
     * </pre>
     */
    public void cqlQuery(mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters request,
        io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.Feature> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_CQL_QUERY, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void spatialQuery(mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters request,
        io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.Feature> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SPATIAL_QUERY, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class VectorQueryBlockingStub extends io.grpc.stub.AbstractStub<VectorQueryBlockingStub> {
    private VectorQueryBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private VectorQueryBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VectorQueryBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new VectorQueryBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * CQL query for vector-based data
     * </pre>
     */
    public java.util.Iterator<mil.nga.giat.geowave.service.grpc.protobuf.Feature> cqlQuery(
        mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_CQL_QUERY, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<mil.nga.giat.geowave.service.grpc.protobuf.Feature> spatialQuery(
        mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SPATIAL_QUERY, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class VectorQueryFutureStub extends io.grpc.stub.AbstractStub<VectorQueryFutureStub> {
    private VectorQueryFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private VectorQueryFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VectorQueryFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new VectorQueryFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_CQL_QUERY = 0;
  private static final int METHODID_SPATIAL_QUERY = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final VectorQueryImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(VectorQueryImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CQL_QUERY:
          serviceImpl.cqlQuery((mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters) request,
              (io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.Feature>) responseObserver);
          break;
        case METHODID_SPATIAL_QUERY:
          serviceImpl.spatialQuery((mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters) request,
              (io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.Feature>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_CQL_QUERY,
        METHOD_SPATIAL_QUERY);
  }

}
