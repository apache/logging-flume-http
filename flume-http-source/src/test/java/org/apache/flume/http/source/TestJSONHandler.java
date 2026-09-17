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
package org.apache.flume.http.source;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jakarta.servlet.http.HttpServletRequest;
import java.lang.reflect.Type;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class TestJSONHandler {

    HTTPSourceHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new JSONHandler();
    }

    @Test
    public void testMultipleEvents() throws Exception {
        String json = "[{\"headers\":{\"a\": \"b\"},\"body\": \"random_body\"},"
                + "{\"headers\":{\"e\": \"f\"},\"body\": \"random_body2\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("random_body", new String(e.getBody(), "UTF-8"));
        e = deserialized.get(1);
        Assertions.assertEquals("f", e.getHeaders().get("e"));
        Assertions.assertEquals("random_body2", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testMultipleEventsUTF16() throws Exception {
        String json = "[{\"headers\":{\"a\": \"b\"},\"body\": \"random_body\"},"
                + "{\"headers\":{\"e\": \"f\"},\"body\": \"random_body2\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json, "UTF-16");
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("random_body", new String(e.getBody(), "UTF-16"));
        e = deserialized.get(1);
        Assertions.assertEquals("f", e.getHeaders().get("e"));
        Assertions.assertEquals("random_body2", new String(e.getBody(), "UTF-16"));
    }

    @Test
    public void testMultipleEventsUTF32() throws Exception {
        String json = "[{\"headers\":{\"a\": \"b\"},\"body\": \"random_body\"},"
                + "{\"headers\":{\"e\": \"f\"},\"body\": \"random_body2\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json, "UTF-32");
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("random_body", new String(e.getBody(), "UTF-32"));
        e = deserialized.get(1);
        Assertions.assertEquals("f", e.getHeaders().get("e"));
        Assertions.assertEquals("random_body2", new String(e.getBody(), "UTF-32"));
    }

    @Test
    public void testMultipleEventsUTF8() throws Exception {
        String json = "[{\"headers\":{\"a\": \"b\"},\"body\": \"random_body\"},"
                + "{\"headers\":{\"e\": \"f\"},\"body\": \"random_body2\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json, "UTF-8");
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("random_body", new String(e.getBody(), "UTF-8"));
        e = deserialized.get(1);
        Assertions.assertEquals("f", e.getHeaders().get("e"));
        Assertions.assertEquals("random_body2", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testEscapedJSON() throws Exception {
        // JSON allows escaping double quotes to add it in the data.
        String json = "[{\"headers\":{\"a\": \"b\"}}," + "{\"headers\":{\"e\": \"f\"},\"body\": \"rand\\\"om_body2\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertTrue(e.getBody().length == 0);
        e = deserialized.get(1);
        Assertions.assertEquals("f", e.getHeaders().get("e"));
        Assertions.assertEquals("rand\"om_body2", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testNoBody() throws Exception {
        String json = "[{\"headers\" : {\"a\": \"b\"}}," + "{\"headers\" : {\"e\": \"f\"},\"body\": \"random_body2\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertTrue(e.getBody().length == 0);
        e = deserialized.get(1);
        Assertions.assertEquals("f", e.getHeaders().get("e"));
        Assertions.assertEquals("random_body2", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testSingleHTMLEvent() throws Exception {
        String json = "[{\"headers\": {\"a\": \"b\"}," + "\"body\": \"<html><body>test</body></html>\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("<html><body>test</body></html>", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testSingleEvent() throws Exception {
        String json = "[{\"headers\" : {\"a\": \"b\"},\"body\": \"random_body\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("random_body", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testBadEvent() throws Exception {
        String json = "{[\"a\": \"b\"],\"body\": \"random_body\"}";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        assertThrows(HTTPBadRequestException.class, () -> handler.getEvents(req));
    }

    @Test
    public void testError() throws Exception {
        String json = "[{\"headers\" : {\"a\": \"b\"},\"body\": \"random_body\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json, "ISO-8859-1");
        assertThrows(UnsupportedCharsetException.class, () -> handler.getEvents(req));
    }

    @Test
    public void testSingleEventInArray() throws Exception {
        String json = "[{\"headers\": {\"a\": \"b\"},\"body\": \"random_body\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("random_body", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testMultipleLargeEvents() throws Exception {
        String json = "[{\"headers\" : {\"a\": \"b\", \"a2\": \"b2\","
                + "\"a3\": \"b3\",\"a4\": \"b4\"},\"body\": \"random_body\"},"
                + "{\"headers\" :{\"e\": \"f\",\"e2\": \"f2\","
                + "\"e3\": \"f3\",\"e4\": \"f4\",\"e5\": \"f5\"},"
                + "\"body\": \"random_body2\"},"
                + "{\"headers\" :{\"q1\": \"b\",\"q2\": \"b2\",\"q3\": \"b3\",\"q4\": \"b4\"},"
                + "\"body\": \"random_bodyq\"}]";
        HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
        List<Event> deserialized = handler.getEvents(req);
        Event e = deserialized.get(0);
        Assertions.assertNotNull(e);
        Assertions.assertEquals("b", e.getHeaders().get("a"));
        Assertions.assertEquals("b2", e.getHeaders().get("a2"));
        Assertions.assertEquals("b3", e.getHeaders().get("a3"));
        Assertions.assertEquals("b4", e.getHeaders().get("a4"));
        Assertions.assertEquals("random_body", new String(e.getBody(), "UTF-8"));
        e = deserialized.get(1);
        Assertions.assertNotNull(e);
        Assertions.assertEquals("f", e.getHeaders().get("e"));
        Assertions.assertEquals("f2", e.getHeaders().get("e2"));
        Assertions.assertEquals("f3", e.getHeaders().get("e3"));
        Assertions.assertEquals("f4", e.getHeaders().get("e4"));
        Assertions.assertEquals("f5", e.getHeaders().get("e5"));
        Assertions.assertEquals("random_body2", new String(e.getBody(), "UTF-8"));
        e = deserialized.get(2);
        Assertions.assertNotNull(e);
        Assertions.assertEquals("b", e.getHeaders().get("q1"));
        Assertions.assertEquals("b2", e.getHeaders().get("q2"));
        Assertions.assertEquals("b3", e.getHeaders().get("q3"));
        Assertions.assertEquals("b4", e.getHeaders().get("q4"));
        Assertions.assertEquals("random_bodyq", new String(e.getBody(), "UTF-8"));
    }

    @Test
    public void testDeserializarion() throws Exception {
        Type listType = new TypeToken<List<JSONEvent>>() {}.getType();
        List<JSONEvent> events = Lists.newArrayList();
        Random rand = new Random();
        for (int i = 1; i < 10; i++) {
            Map<String, String> input = Maps.newHashMap();
            for (int j = 1; j < 10; j++) {
                input.put(String.valueOf(i) + String.valueOf(j), String.valueOf(i));
            }
            JSONEvent e = new JSONEvent();
            e.setBody(String.valueOf(rand.nextGaussian()).getBytes("UTF-8"));
            e.setHeaders(input);
            events.add(e);
        }
        Gson gson = new Gson();
        List<Event> deserialized = handler.getEvents(new FlumeHttpServletRequestWrapper(gson.toJson(events, listType)));
        int i = 0;
        for (Event e : deserialized) {
            Event current = events.get(i++);
            Assertions.assertEquals(new String(current.getBody(), "UTF-8"), new String(e.getBody(), "UTF-8"));
            Assertions.assertEquals(current.getHeaders(), e.getHeaders());
        }
    }
}
