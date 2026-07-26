// 监听 rpc 调用

package master

import ( 
	"context"
	"log"
	"net"
	pb "github.com/mapreduce_impl/rpc"
	"google.golang.org/grpc"

)
func (master *Master) RpcServiceCall(ctx context.Context) {
	// 1. 创建 gRPC 实例
	grpcServer := grpc.NewServer()

	// 2. 注册服务
	pb.RegisterMasterServiceServer(grpcServer, master)

	// 3. 指定端口上创建 TCP 监听器监听服务
	listen, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("[TCP Service] TCP service start error")
	}

	err = grpcServer.Serve(listen)  // 内部调用 Accept() 阻塞代码
	if err != nil {
		log.Fatal("[TCP Service] gRPC listen port error")
	}
	log.Println("[TCP Service] TCP service listening  on port 50051")
	
}
