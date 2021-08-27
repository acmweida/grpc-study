package com.grpc.study.experimental;

import io.grpc.*;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class CompressingHelloWorldServerPerMethod {
    private static final Logger logger = Logger.getLogger(CompressingHelloWorldServerPerMethod.class.getName());

    private Server server;

    public void start() throws IOException {
        int port = 50052;
        server = ServerBuilder.forPort(port)
                .addService(new GreeterGrpcImpl())
                .build().start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    CompressingHelloWorldServerPerMethod.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        CompressingHelloWorldServerPerMethod server = new CompressingHelloWorldServerPerMethod();
        server.start();
        server.blockUntilShutdown();
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class GreeterGrpcImpl extends GreeterGrpc.GreeterImplBase {

        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            System.out.println("recive message :" + request.getName());

            // 强转
            ServerCallStreamObserver<HelloReply> cell = (ServerCallStreamObserver<HelloReply>) responseObserver;
            cell.setCompression("gzip");


            HelloReply reply = HelloReply.newBuilder().setMessage("hello"+request.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
