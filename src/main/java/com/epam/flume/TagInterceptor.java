package com.epam.flume;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interceptor to add real tags based on tag Id
 */
public class TagInterceptor implements Interceptor {

    private static final Logger LOG = Logger.getLogger(TagInterceptor.class);

    public static final String TAG_FILE_SEPARATOR = "\t";
    public static final int TAG_FILE_COLUMN_NUMBER = 6;
    public static final int TAG_FILE_ID_COLUMN = 0;
    public static final int TAG_FILE_TAGS_COLUMN = 1;
    public static final String EVENT_BODY_VALUES_SEPARATOR = "\t";
    public static final int EVENT_BODY_COLUMNS_NUMBER = 22;
    public static final int EVENT_BODY_TAG_ID_COLUMN_NUMBER = 20;
    public static final int EVENT_BODY_TIMESTAMP_COLUMN_NUMBER = 1;

    public static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyyMMddhhmmsss");

    private String tagsHeader;
    private String tagsFilePath;
    private Map<String, String> tagsMap;

    private TagInterceptor(String tagsHeader, String tagsFilePath) {
        this.tagsHeader = tagsHeader;
        this.tagsFilePath = tagsFilePath;
        this.tagsMap = new HashMap<>();
    }

    public void initialize() {
        LOG.info("initialize...");
        File tagsFile = new File(tagsFilePath);
        if (tagsFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(tagsFile))) {
                String line;
                boolean firstLineRead = false;
                while ((line = reader.readLine()) != null) {
                    //skip first line with headers
                    if (!firstLineRead) {
                        firstLineRead = true;
                        continue;
                    }
                    String[] columnValues = line.split(TAG_FILE_SEPARATOR, -1);
                    if (columnValues.length != TAG_FILE_COLUMN_NUMBER) {
                        //log invalid column
                        //skip line processing
                        continue;
                    }
                    String id = columnValues[TAG_FILE_ID_COLUMN];
                    String tags = columnValues[TAG_FILE_TAGS_COLUMN];
                    tagsMap.put(id, tags);
                }
            } catch (FileNotFoundException e) {
                LOG.error(e);
                throw new FlumeException("File: " + tagsFilePath + " with user tags is not exist. Please specify correct file path.");
            } catch (IOException e) {
                LOG.error(e);
                throw new FlumeException(e);
            }
        } else {
            throw new FlumeException("File: " + tagsFilePath + " with user tags is not exist. Please specify correct file path.");
        }
    }

    public Event intercept(Event event) {
        LOG.info("intercepting...");
        String body = new String(event.getBody());
        Map<String, String> headers = event.getHeaders();

        //parse body
        String[] eventColumnValues = body.split(EVENT_BODY_VALUES_SEPARATOR, -1);
        if (eventColumnValues.length == EVENT_BODY_COLUMNS_NUMBER) {

            String timestamp = eventColumnValues[EVENT_BODY_TIMESTAMP_COLUMN_NUMBER];
            long ts;
            try {
                ts = DATE_FORMATTER.parse(timestamp).getTime();
                headers.put(TimestampInterceptor.Constants.TIMESTAMP, Long.toString(ts));
            } catch (ParseException e) {
                LOG.error(e);
                throw new FlumeException(e);
            }

            String tagsId = eventColumnValues[EVENT_BODY_TAG_ID_COLUMN_NUMBER];
            String tags = tagsMap.get(tagsId);
            if (StringUtils.isNotBlank(tags)) {
                headers.put(tagsHeader, Boolean.TRUE.toString());
            } else {
                headers.put(tagsHeader, Boolean.FALSE.toString());
            }
            body = body + EVENT_BODY_VALUES_SEPARATOR + (tags == null? StringUtils.EMPTY : tags);
        }

        LOG.info(body);
        event.setBody(body.getBytes());

        return event;
    }

    public List<Event> intercept(List<Event> list) {
        List<Event> interceptedEvents = new ArrayList<>(list.size());
        for (Event event: list) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    public void close() {
        //do nothing
    }

    public static class Builder implements Interceptor.Builder {

        private String tagsHeader;
        private String tagsFilePath;

        @Override
        public Interceptor build() {
            return new TagInterceptor(tagsHeader, tagsFilePath);
        }

        @Override
        public void configure(Context context) {
            tagsHeader = context.getString("tagsHeader");
            tagsFilePath = context.getString("tagsFilePath");
        }
    }
}
