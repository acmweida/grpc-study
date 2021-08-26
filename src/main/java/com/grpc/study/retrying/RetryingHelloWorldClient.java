package com.grpc.study.retrying;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;

import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RetryingHelloWorldClient {
    static final String ENV_DISABLE_RETRYING = "DISABLE_RETRYING_IN_RETRYING_EXAMPLE";

    private static final Logger logger = Logger.getLogger(RetryingHelloWorldClient.class.getName());

    private boolean enableRetries;
    private final ManagedChannel channel;
    private final GreeterGrpc.GreeterBlockingStub blockingStub;
    private final AtomicInteger totalRpcs = new AtomicInteger(0);
    private final AtomicInteger failedRpcs = new AtomicInteger(0);

    protected Map<String, ?> getRetryingServiceConfig() {
        return new Gson()
                .fromJson(
                        new JsonReader(
                                new InputStreamReader(
                                        RetryingHelloWorldClient.class.getResourceAsStream(
                                                "/retrying/retrying_service_config.json"
                                        )
                                        ,UTF_8)
                        )
                        ,Map.class);
    }

    public RetryingHelloWorldClient(String host,int port,boolean enableRetries) {
        ManagedChannelBuilder builder = ManagedChannelBuilder.forAddress(host,port)
                .usePlaintext();
        if (enableRetries) {
            Map<String, ?> retryingServiceConfig = getRetryingServiceConfig();
            builder.defaultServiceConfig(retryingServiceConfig).enableRetry();
        }
        channel = builder.build();
        blockingStub = GreeterGrpc.newBlockingStub(channel);
        this.enableRetries = enableRetries;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(60, TimeUnit.SECONDS);
    }

    public void greet(String name) {
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        HelloReply reply = null;
        StatusRuntimeException exception = null;
        try {
            reply = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            exception = e;
            failedRpcs.incrementAndGet();
        }

        totalRpcs.incrementAndGet();
        if (exception == null) {
            logger.log(Level.INFO,"Greeting: {0}", new Object[]{reply.getMessage()});
        } else {
            logger.log(Level.INFO,"RPC failed: {0}", new Object[]{exception.getStatus()});
        }
    }

    private void printSummary() {
        logger.log(
                Level.INFO,
                "\n\nTotal RPCs sent: {0}. Total RPCs failed: {1}\n",
                new Object[]{
                        totalRpcs.get(), failedRpcs.get()});

        if (enableRetries) {
            logger.log(
                    Level.INFO,
                    "Retrying enabled. To disable retries, run the client with environment variable {0}=true.",
                    ENV_DISABLE_RETRYING);
        } else {
            logger.log(
                    Level.INFO,
                    "Retrying disabled. To enable retries, unset environment variable {0} and then run the client.",
                    ENV_DISABLE_RETRYING);
        }
    }


    public static void main(String[] args) throws InterruptedException {
        boolean enableRetries = true;
        RetryingHelloWorldClient client = new RetryingHelloWorldClient("127.0.0.1", 50052, enableRetries);
        ForkJoinPool executor = new ForkJoinPool();

        for (int i = 0; i < 50; i++) {
            final String userId = "user" + i;
            executor.execute(() ->
                    client.greet(userId)
            );
        }
        executor.awaitTermination(10, TimeUnit.SECONDS);
        executor.shutdown();
        client.printSummary();
        client.shutdown();
    }

}
