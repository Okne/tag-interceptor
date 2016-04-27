package com.epam.flume;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.Mockito.doReturn;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple TagInterceptor.
 */
@RunWith(MockitoJUnitRunner.class)
public class TagInterceptorTest {

    public static final String EVENT_BODY_1 = "92edf3c30e623ca03bdafa515a5bf5b2\t20130609000103422\tVhkxLeKnPl1LtCC\tmozilla/4.0 "
            + "(compatible; msie 6.0; windows nt 5.1; sv1; .net clr 2.0.50727; 360se)\t14.112.208.*\t216\t227\t1\ttrqRTvpWM9so1m5b\t"
            + "e3bc7499f018a4fe40a87d64e2013944\t\tmm_10009936_105923_10950458\t300\t250\t2\t1\t0\td881a6c788e76c2c27ed1ef04f119544\t227\t3358\t282162975533\t1";
    public static final String EVENT_1_TAGS = "monitor,av,car,inch,two,way";
    public static final String EVENT_BODY_2 = "92edf3c30e623ca03bdafa515a5bf5b2\t20130610000103422\tVhkxLeKnPl1LtCC\tmozilla/4.0 "
            + "(compatible; msie 6.0; windows nt 5.1; sv1; .net clr 2.0.50727; 360se)\t14.112.208.*\t216\t227\t1\ttrqRTvpWM9so1m5b\t"
            + "e3bc7499f018a4fe40a87d64e2013944\t\tmm_10009936_105923_10950458\t300\t250\t2\t1\t0\td881a6c788e76c2c27ed1ef04f119544\t227\t3358\tnot-existing-user-tag-id\t1";

    public static final String BODY_VALUES_SEP = "\t";
    public static final int BODY_VALUES_NUMBER = 23;
    public static final int BODY_TAGS_COLUMN_NUMBER = 22;
    public static final String WITH_REAL_TAGS = "withRealTags";

    Interceptor testObj;
    TagInterceptor.Builder builder = new TagInterceptor.Builder();

    @Mock Context context;

    @Before
    public void setup() throws Exception {
        doReturn(WITH_REAL_TAGS).when(context).getString("tagsHeader");
        doReturn("user.profile.tags.us.txt").when(context).getString("tagsFilePath");

        builder.configure(context);
        testObj = builder.build();
        testObj.initialize();
    }

    @Test
    public void intercept_happyDay_test() throws Exception {
        //given
        Event event = new SimpleEvent();
        event.setBody(EVENT_BODY_1.getBytes());

        //when
        Event interceptedEvent = testObj.intercept(event);

        //then
        assertNotNull(interceptedEvent);
        byte[] body = interceptedEvent.getBody();
        Map<String, String> headers = interceptedEvent.getHeaders();
        assertNotNull(body);
        assertNotNull(headers);
        assertTrue(headers.containsKey(WITH_REAL_TAGS));
        assertEquals(Boolean.TRUE.toString(), headers.get(WITH_REAL_TAGS));
        assertTrue(headers.containsKey(TimestampInterceptor.Constants.TIMESTAMP));
        assertEquals("1370750282000", headers.get(TimestampInterceptor.Constants.TIMESTAMP));
        assertEquals(new String(body).split(BODY_VALUES_SEP, -1).length, BODY_VALUES_NUMBER);
        assertEquals(new String(body).split(BODY_VALUES_SEP, -1)[BODY_TAGS_COLUMN_NUMBER], EVENT_1_TAGS);
    }

    @Test
    public void intercept_noTagsForId_test() throws Exception {
        //given
        Event event = new SimpleEvent();
        event.setBody(EVENT_BODY_2.getBytes());

        //when
        Event interceptedEvent = testObj.intercept(event);

        //then
        assertNotNull(interceptedEvent);
        byte[] body = interceptedEvent.getBody();
        Map<String, String> headers = interceptedEvent.getHeaders();
        assertNotNull(body);
        assertNotNull(headers);
        assertTrue(headers.containsKey(WITH_REAL_TAGS));
        assertEquals(Boolean.FALSE.toString(), headers.get(WITH_REAL_TAGS));
        assertTrue(headers.containsKey(TimestampInterceptor.Constants.TIMESTAMP));
        assertEquals("1370836682000", headers.get(TimestampInterceptor.Constants.TIMESTAMP));
        assertEquals(new String(body).split(BODY_VALUES_SEP, -1).length, BODY_VALUES_NUMBER);
        assertEquals(new String(body).split(BODY_VALUES_SEP, -1)[BODY_TAGS_COLUMN_NUMBER], StringUtils.EMPTY);
    }
}
