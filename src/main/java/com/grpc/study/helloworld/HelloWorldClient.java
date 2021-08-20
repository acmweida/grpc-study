package com.grpc.study.helloworld;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class HelloWorldClient {

    private static final Logger logger = Logger.getLogger(HelloWorldClient.class.getName());

    private final GreeterGrpc.GreeterBlockingStub blockingStub;

    public HelloWorldClient(Channel channel) {
        blockingStub =  GreeterGrpc.newBlockingStub(channel);
    }

    public void greate(String name) {
        HelloRequest request= HelloRequest.newBuilder().setName(name).build();
        HelloReply response = null;
        response = blockingStub.sayHello(request);

        System.out.println(response.getMessage());
    }

    public static void main(String[] args) throws InterruptedException {
        String user = "hello world grpc";
        String target = "127.0.0.1:50052";

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        HelloWorldClient client = new HelloWorldClient(channel);

        client.greate(user);

        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
}
