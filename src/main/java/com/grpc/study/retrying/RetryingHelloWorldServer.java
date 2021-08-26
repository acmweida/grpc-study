package com.grpc.study.retrying;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 重试
 */
public class RetryingHelloWorldServer {

    private static final Logger logger = Logger.getLogger(RetryingHelloWorldServer.class.getName());

    private static final float UNAVAILABLE_PERCENTAGE = 0.5F; // 失败因子
    private static final Random random = new Random();

    private Server server;

    DecimalFormat df = new DecimalFormat("#%");

    public void start() throws IOException {
        int port = 50052;
        server = ServerBuilder.forPort(port)
                .addService(new GreeterImpl())
                .build().start();
        logger.info("Responding as UNAVAILABLE to " + df.format(UNAVAILABLE_PERCENTAGE) + " requests");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    RetryingHelloWorldServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RetryingHelloWorldServer server = new RetryingHelloWorldServer();
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

    static class GreeterImpl extends GreeterGrpc.GreeterImplBase {
        AtomicInteger retryCount = new AtomicInteger(0);


        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            int count = retryCount.incrementAndGet();
            if (random.nextFloat() < UNAVAILABLE_PERCENTAGE) {
                logger.info("Returning stubbed UNAVAILABLE error. count: " + count);
                responseObserver.onError(Status.UNAVAILABLE.withDescription("Greeter temporarily unavailable...").asRuntimeException());
            } else {
                logger.info("Returning successful Hello response, count: " + count);
                HelloReply reply = HelloReply.newBuilder().setMessage("hello "+request.getName()).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        }
    }


}
