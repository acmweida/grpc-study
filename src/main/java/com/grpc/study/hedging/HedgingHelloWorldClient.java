package com.grpc.study.hedging;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;

import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HedgingHelloWorldClient {

    static final String ENV_DISABLE_HEDGING = "DISABLE_HEDGING_IN_HEDGING_EXAMPLE";

    private static final Logger logger = Logger.getLogger(HedgingHelloWorldClient.class.getName());

    private boolean hedging;
    private ManagedChannel channel;
    private GreeterGrpc.GreeterBlockingStub blockingStub;
    private final PriorityBlockingQueue<Long> latencies = new PriorityBlockingQueue<>();
    private final AtomicInteger failedRpcs = new AtomicInteger();

    public HedgingHelloWorldClient(String host,int port,boolean hedging) {
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host,port).usePlaintext();

        if (hedging) {
            Map<String,?> hedingServiceConfig =
                    new Gson().fromJson(
                            new JsonReader(
                                new InputStreamReader(
                                        Objects.requireNonNull(HedgingHelloWorldClient.class.getResourceAsStream(
                                                "/hedging/hedging_service_config.json"
                                        ))
                                , StandardCharsets.UTF_8)
                            )
                            ,Map.class);
            channelBuilder.defaultServiceConfig(hedingServiceConfig);
        }
        channel = channelBuilder.build();
        blockingStub = GreeterGrpc.newBlockingStub(channel);
        this.hedging = hedging;
    }


    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void greet(String name) {
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        HelloReply response =null;
        StatusRuntimeException statusRuntimeException = null;
        long startTime = System.nanoTime();
        try {
            response = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            failedRpcs.incrementAndGet();
            statusRuntimeException = e;
        }
        long latencyMills =TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-startTime);
        latencies.add(latencyMills);

        if (statusRuntimeException == null) {
            logger.log(
                    Level.INFO,
                    "Greeting: {0}. Latency: {1}ms",
                    new Object[] {response.getMessage(), latencyMills});
        } else {
            logger.log(
                    Level.INFO,
                    "RPC failed: {0}. Latency: {1}ms",
                    new Object[] {statusRuntimeException.getStatus(), latencyMills});
        }
    }

    void printSummary() {
        int rpcCount = latencies.size();
        long latency50 = 0L;
        long latency90 = 0L;
        long latency95 = 0L;
        long latency99 = 0L;
        long latency999 = 0L;
        long latencyMax = 0L;
        for (int i = 0; i < rpcCount; i++) {
            long latency = latencies.poll();
            if (i == rpcCount * 50 / 100 - 1) {
                latency50 = latency;
            }
            if (i == rpcCount * 90 / 100 - 1) {
                latency90 = latency;
            }
            if (i == rpcCount * 95 / 100 - 1) {
                latency95 = latency;
            }
            if (i == rpcCount * 99 / 100 - 1) {
                latency99 = latency;
            }
            if (i == rpcCount * 999 / 1000 - 1) {
                latency999 = latency;
            }
            if (i == rpcCount - 1) {
                latencyMax = latency;
            }
        }

        logger.log(
                Level.INFO,
                "\n\nTotal RPCs sent: {0}. Total RPCs failed: {1}\n"
                        + (hedging ? "[Hedging enabled]\n" : "[Hedging disabled]\n")
                        + "========================\n"
                        + "50% latency: {2}ms\n"
                        + "90% latency: {3}ms\n"
                        + "95% latency: {4}ms\n"
                        + "99% latency: {5}ms\n"
                        + "99.9% latency: {6}ms\n"
                        + "Max latency: {7}ms\n"
                        + "========================\n",
                new Object[]{
                        rpcCount, failedRpcs.get(),
                        latency50, latency90, latency95, latency99, latency999, latencyMax});

        if (hedging) {
            logger.log(
                    Level.INFO,
                    "To disable hedging, run the client with environment variable {0}=true.",
                    ENV_DISABLE_HEDGING);
        } else {
            logger.log(
                    Level.INFO,
                    "To enable hedging, unset environment variable {0} and then run the client.",
                    ENV_DISABLE_HEDGING);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        HedgingHelloWorldClient client = new HedgingHelloWorldClient("127.0.0.1",50052,true);
        ForkJoinPool pool = new ForkJoinPool();

        for (int i=0;i<2000;i++) {
            final String userId = "user" + i;
            pool.execute(() -> client.greet(userId));
        }
        pool.awaitQuiescence(100,TimeUnit.SECONDS);
        pool.shutdown();
        client.printSummary();
        client.shutdown();
    }

}
