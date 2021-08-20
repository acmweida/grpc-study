package com.grpc.study.retrying;

import java.util.Random;
import java.util.logging.Logger;

public class RetryingHelloWorldServer {

    private static final Logger logger = Logger.getLogger(RetryingHelloWorldServer.class.getName());

    private static final float UNAVAILABLE_PERCENTAGE = 0.5F; // 失败因子
    private static final Random random = new Random();
}
