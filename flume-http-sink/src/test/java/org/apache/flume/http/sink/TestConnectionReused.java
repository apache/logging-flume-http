/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.http.sink;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.flume.Sink.Status;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ServeEventListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/* Note: This test was moved from TestHttpSinkIT since it used constructs from
  previous versions of Wiremock that are no longer supported and it is possible
  this might have different behavior on different systems.
*/
class TestConnectionReused {

    private static CountDownLatch firstRequestReceived;
    private static final int CONNECT_TIMEOUT = 2500;

    static {
        // Disable HTTP keep-alive so that each request opens a fresh connection.
        // Without this, the JDK's HttpURLConnection connection pool can reuse a
        // connection whose response is still being delivered late by WireMock
        // (due to the deliberately delayed response below), causing the second
        // request in this test to be entangled with the leftover state of the
        // first and produce unpredictable timing.
        System.setProperty("http.keepAlive", "false");
    }

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("Can not find free port.", e);
        }
    }

    private static final int port = findFreePort();

    // Register the Extension and include a custom ServeEventListener
    @RegisterExtension
    static WireMockExtension service = WireMockExtension.newInstance()
            .options(wireMockConfig().port(port).extensions(new ServeEventListener() {
                @Override
                public String getName() {
                    return "timeout-reset-listener";
                }

                @Override
                public void afterComplete(ServeEvent serveEvent, Parameters parameters) {
                    // WireMock 3: Use the instance client to reset the delay safely
                    service.getRuntimeInfo().getWireMock().setGlobalFixedDelayVariable(0);

                    if (firstRequestReceived != null) {
                        firstRequestReceived.countDown();
                    }
                }
            }))
            .build();

    private MemoryChannel channel;

    private HttpSink httpSink;

    @BeforeEach
    public void setupSink() {
        firstRequestReceived = new CountDownLatch(1);
        if (httpSink == null) {
            Context httpSinkContext = new Context();
            httpSinkContext.put("endpoint", "http://localhost:" + port + "/endpoint");
            httpSinkContext.put("requestTimeout", "2000");
            httpSinkContext.put("connectTimeout", "1500");
            httpSinkContext.put("acceptHeader", "application/json");
            httpSinkContext.put("contentTypeHeader", "application/json");
            httpSinkContext.put("backoff.200", "false");
            httpSinkContext.put("rollback.200", "false");
            httpSinkContext.put("backoff.401", "false");
            httpSinkContext.put("rollback.401", "false");
            httpSinkContext.put("incrementMetrics.200", "true");

            Context memoryChannelContext = new Context();

            channel = new MemoryChannel();
            channel.configure(memoryChannelContext);
            channel.start();

            httpSink = new HttpSink();
            httpSink.configure(httpSinkContext);
            httpSink.setChannel(channel);
            httpSink.start();
        }
    }

    @AfterEach
    public void waitForShutdown() throws InterruptedException {
        httpSink.stop();
        Thread.sleep(500);
    }

    @Test
    public void ensureHttpConnectionReusedForSuccessfulRequests() throws Exception {
        // we should only get one delay when establishing a connection
        service.getRuntimeInfo().getWireMock().setGlobalFixedDelayVariable(1000);

        service.stubFor(post(urlEqualTo("/endpoint"))
                .withRequestBody(equalToJson(event("SUCCESS")))
                .willReturn(aResponse().withStatus(200)));

        long startTime = System.currentTimeMillis();

        addEventToChannel(event("SUCCESS"), Status.READY);
        addEventToChannel(event("SUCCESS"), Status.READY);
        addEventToChannel(event("SUCCESS"), Status.READY);

        long endTime = System.currentTimeMillis();
        assertTrue(endTime - startTime < 2500, "Test should have completed faster");

        service.verify(3, postRequestedFor(urlEqualTo("/endpoint")).withRequestBody(equalToJson(event("SUCCESS"))));
    }

    private void addEventToChannel(String line, Status expectedStatus) throws EventDeliveryException {

        SimpleEvent event = new SimpleEvent();
        event.setBody(line.getBytes());

        Transaction channelTransaction = channel.getTransaction();
        channelTransaction.begin();
        channel.put(event);
        channelTransaction.commit();
        channelTransaction.close();

        Status status = httpSink.process();

        assertEquals(expectedStatus, status);
    }

    private String event(String id) {
        return "{'id':'" + id + "'}";
    }
}
