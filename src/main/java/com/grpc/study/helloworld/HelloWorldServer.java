package com.grpc.study.helloworld;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class HelloWorldServer {

    private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

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
                    HelloWorldServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        HelloWorldServer server = new HelloWorldServer();
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

            HelloReply reply = HelloReply.newBuilder().setMessage("hello"+request.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

}
