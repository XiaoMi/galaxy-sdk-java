package com.xiaomi.infra.galaxy.emq.example;

import java.util.ArrayList;
import java.util.List;

import com.xiaomi.infra.galaxy.emq.thrift.*;
import libthrift091.TException;

import com.xiaomi.infra.galaxy.emq.client.EMQClientFactory;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

/* This JAVA SDK support Java6 and Java7; Java8 is not included yet */

public class Main {
    private static String secretKeyId = "AKA2IUZH2BM3X6MZS3"; // Set your AppKey, like "5521728135794"

    private static String secretKey = "JPkwtoHQYHS0dEYMwF0iCfYb9ti8YUgpgaICkAOa"; // Set your AppSecret, like "K7czwCuHttwZD49DD/qKzg=="
    private static String name = "testClient";

    public static void main(String[] args) {
        Credential credential = new Credential().setSecretKeyId(secretKeyId).
                setSecretKey(secretKey).setType(UserType.APP_SECRET);
        EMQClientFactory clientFactory = new EMQClientFactory(credential);
        QueueService.Iface queueClient = clientFactory.newQueueClient(
                "http://cnbj1-emq.api.xiaomi.net");
        MessageService.Iface messageClient = clientFactory.newMessageClient(
                "http://cnbj1-emq.api.xiaomi.net");

        try {
            String queueName = "CL968/priorityQueue123";
            ListTagRequest ltr = new ListTagRequest();
            ltr.setQueueName("CL968/priorityQueue123");
            ListTagResponse listTagResponse = queueClient.listTag(ltr);
            System.out.println(listTagResponse);

            GetTagInfoRequest gtr = new GetTagInfoRequest();
            gtr.setQueueName("CL968/priorityQueue123");
            gtr.setTagName("");
            GetTagInfoResponse getTagInfoResponse = queueClient.getTagInfo(gtr);
            System.out.println(getTagInfoResponse);

        } catch (Exception e) {
            if (e instanceof GalaxyEmqServiceException) {
                GalaxyEmqServiceException ex = (GalaxyEmqServiceException) e;
                System.out.printf("Failed. Reason:" + ex.getErrMsg() + "\n" +
                        ex.getDetails() + " requestId=" + ex.getRequestId() + "\n\n");
            } else {
                System.out.printf("Failed." + e.getMessage() + "\n\n");
            }
        }
    }
}
