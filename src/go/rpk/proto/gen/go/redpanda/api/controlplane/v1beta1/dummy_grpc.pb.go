// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: redpanda/api/controlplane/v1beta1/dummy.proto

package controlplanev1beta1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	DummyService_DummyMethod_FullMethodName = "/redpanda.api.controlplane.v1beta1.DummyService/DummyMethod"
)

// DummyServiceClient is the client API for DummyService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DummyServiceClient interface {
	// buf:lint:ignore RPC_REQUEST_STANDARD_NAME
	// buf:lint:ignore RPC_RESPONSE_STANDARD_NAME
	// buf:lint:ignore RPC_REQUEST_RESPONSE_UNIQUE
	DummyMethod(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*DummyMethodResponse, error)
}

type dummyServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDummyServiceClient(cc grpc.ClientConnInterface) DummyServiceClient {
	return &dummyServiceClient{cc}
}

func (c *dummyServiceClient) DummyMethod(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*DummyMethodResponse, error) {
	out := new(DummyMethodResponse)
	err := c.cc.Invoke(ctx, DummyService_DummyMethod_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DummyServiceServer is the server API for DummyService service.
// All implementations should embed UnimplementedDummyServiceServer
// for forward compatibility
type DummyServiceServer interface {
	// buf:lint:ignore RPC_REQUEST_STANDARD_NAME
	// buf:lint:ignore RPC_RESPONSE_STANDARD_NAME
	// buf:lint:ignore RPC_REQUEST_RESPONSE_UNIQUE
	DummyMethod(context.Context, *emptypb.Empty) (*DummyMethodResponse, error)
}

// UnimplementedDummyServiceServer should be embedded to have forward compatible implementations.
type UnimplementedDummyServiceServer struct {
}

func (UnimplementedDummyServiceServer) DummyMethod(context.Context, *emptypb.Empty) (*DummyMethodResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DummyMethod not implemented")
}

// UnsafeDummyServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DummyServiceServer will
// result in compilation errors.
type UnsafeDummyServiceServer interface {
	mustEmbedUnimplementedDummyServiceServer()
}

func RegisterDummyServiceServer(s grpc.ServiceRegistrar, srv DummyServiceServer) {
	s.RegisterService(&DummyService_ServiceDesc, srv)
}

func _DummyService_DummyMethod_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DummyServiceServer).DummyMethod(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DummyService_DummyMethod_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DummyServiceServer).DummyMethod(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// DummyService_ServiceDesc is the grpc.ServiceDesc for DummyService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DummyService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "redpanda.api.controlplane.v1beta1.DummyService",
	HandlerType: (*DummyServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DummyMethod",
			Handler:    _DummyService_DummyMethod_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "redpanda/api/controlplane/v1beta1/dummy.proto",
}
