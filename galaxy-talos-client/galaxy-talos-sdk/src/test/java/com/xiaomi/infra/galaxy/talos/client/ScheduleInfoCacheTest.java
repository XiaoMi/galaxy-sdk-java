/**
 * Copyright 2018, Xiaomi. All rights reserved. Author: zhangqian8@xiaomi.com
 */
package com.xiaomi.infra.galaxy.talos.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.thrift.GetScheduleInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetScheduleInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class ScheduleInfoCacheTest {
    private static final String topicName = "MyTopic1";
    private static final String resourceName = "12345#MyTopic1#dfi34598dfj4";
    private static final String topicName2 = "MyTopic2";
    private static final String resourceName2 = "12345#MyTopic2#dfi34598dfj4";
    private static final String host0 = "host0URL";
    private static final String host1 = "host1URL";
    private static final String host2 = "host2URL";
    private static final int partitionId = 7;

    private static TalosClientFactory talosClientFactoryMock;
    private static TalosClientConfig talosClientConfig;
    private static MessageService.Iface messageClientMock;
    private static MessageService.Iface messageClientMock0;
    private static MessageService.Iface messageClientMock1;
    private static MessageService.Iface messageClientMock2;
    private static GetScheduleInfoResponse responseMock;
    private static TopicAndPartition topicAndPartition;
    private static TopicAndPartition topicAndPartition0;
    private static TopicAndPartition topicAndPartition1;
    private static TopicAndPartition topicAndPartition2;

    private static Map<TopicAndPartition, String> topicScheduleInfoMap1;
    private static Map<TopicAndPartition, String> topicScheduleInfoMap2;

    @Before
    public void setUp() throws IOException {
        Properties properties = new Properties();
        properties.setProperty("galaxy.talos.service.endpoint", "testUrl");
        talosClientConfig = new TalosClientConfig(properties);
        talosClientConfig.setAutoLocation(true);
        topicAndPartition = new TopicAndPartition(topicName,
            new TopicTalosResourceName(resourceName), partitionId);
        topicAndPartition0 = new TopicAndPartition(topicName2,
            new TopicTalosResourceName(resourceName2), 0);
        topicAndPartition1 = new TopicAndPartition(topicName2,
            new TopicTalosResourceName(resourceName2), 1);
        topicAndPartition2 = new TopicAndPartition(topicName2,
            new TopicTalosResourceName(resourceName2), 2);
        messageClientMock = Mockito.mock(MessageService.Iface.class);
        messageClientMock0 = Mockito.mock(MessageService.Iface.class);
        messageClientMock1 = Mockito.mock(MessageService.Iface.class);
        messageClientMock2 = Mockito.mock(MessageService.Iface.class);
        responseMock = Mockito.mock(GetScheduleInfoResponse.class);
        talosClientFactoryMock = Mockito.mock(TalosClientFactory.class);
        topicScheduleInfoMap1 = new HashMap<TopicAndPartition, String>();
        topicScheduleInfoMap1.put(topicAndPartition0, host0);
        topicScheduleInfoMap1.put(topicAndPartition1, host1);
        topicScheduleInfoMap1.put(topicAndPartition2, host2);
        topicScheduleInfoMap2 = new HashMap<TopicAndPartition, String>();
        topicScheduleInfoMap2.put(topicAndPartition1, host0);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testGetScheduleInfoCache() throws Exception {
        ScheduleInfoCache scheduleInfoCache1 = ScheduleInfoCache.getScheduleInfoCache(new TopicTalosResourceName(resourceName),
            talosClientConfig, messageClientMock, talosClientFactoryMock);
        ScheduleInfoCache scheduleInfoCache2 = ScheduleInfoCache.getScheduleInfoCache(new TopicTalosResourceName(resourceName),
            talosClientConfig, messageClientMock, talosClientFactoryMock);
        assertNotNull(scheduleInfoCache1);
        assertEquals(scheduleInfoCache1, scheduleInfoCache2);
    }

    @Test
    public void testGetOrCreateMessageClient() throws Exception {
        when(messageClientMock.getScheduleInfo(any(GetScheduleInfoRequest.class))).thenReturn(responseMock);
        when(responseMock.getScheduleInfo()).thenReturn(topicScheduleInfoMap1);

        when(talosClientFactoryMock.newMessageClient("http://" + host0)).thenReturn(messageClientMock0);
        when(talosClientFactoryMock.newMessageClient("http://" + host1)).thenReturn(messageClientMock1);
        when(talosClientFactoryMock.newMessageClient("http://" + host2)).thenReturn(messageClientMock2);

        ScheduleInfoCache scheduleInfoCache = ScheduleInfoCache.getScheduleInfoCache(new TopicTalosResourceName(resourceName2),
            talosClientConfig, messageClientMock, talosClientFactoryMock);
        //scheduleInfoMap.get(topicAndPartition) is null, at the same time test UpdatescheduleInfoCache
        assertEquals(messageClientMock, scheduleInfoCache.getOrCreateMessageClient(topicAndPartition));
        //scheduleInfoMap.get(topicAndPartition1) return messageClientMock1
        assertEquals(messageClientMock1, scheduleInfoCache.getOrCreateMessageClient(topicAndPartition1));


        //test UpdatescheduleInfoCache
        when(responseMock.getScheduleInfo()).thenReturn(topicScheduleInfoMap2);
        scheduleInfoCache.updatescheduleInfoCache();
        Thread.sleep(10);
        assertEquals(messageClientMock0, scheduleInfoCache.getOrCreateMessageClient(topicAndPartition1));
    }

}
