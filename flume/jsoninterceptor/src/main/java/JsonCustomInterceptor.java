package jvolkova.gridu;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class JsonCustomInterceptor implements Interceptor {


    private static String header = null;
    private static String defaultValue = null;
    private static String defaultFieldKey = null;
    private static String timeStampField = null;

    private final String KAFKA_KEY = "key";
    private final String TIMESTAMP_KEY = "timestamp";

    static Logger logger = Logger.getLogger(JsonCustomInterceptor.class.getName());

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event)
    {
        if(defaultValue != null || defaultFieldKey != null) {

            String keyValue = null;
            String type = defaultValue;
            String timeStamp = defaultValue;

            byte[] eventBody = event.getBody();

            Map headers = event.getHeaders();

            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(new String(eventBody));
                if(timeStampField != null) {
                    timeStamp = rootNode.get(timeStampField).asText();
                    headers.put(TIMESTAMP_KEY, timeStamp);
                    ((ObjectNode)rootNode).remove(timeStampField);
                }
                if(defaultValue != null){
                    type = rootNode.get(header).asText();
                    headers.put(header, type);
                    ((ObjectNode)rootNode).remove(header);
                }
                if(defaultFieldKey != null) {
                    keyValue = rootNode.get(defaultFieldKey).asText();
                    headers.put(KAFKA_KEY, keyValue);
                    ((ObjectNode)rootNode).remove(defaultFieldKey);
                }
                byte[] eventEditValue = mapper.writeValueAsBytes(rootNode);
                event.setBody(eventEditValue);
            } catch (JsonProcessingException e) {
                logger.warning("JsonProcessingException: " + e.getMessage() + "Event: " + event);
            } catch (Exception e) {
                logger.warning("Exception Class: " + e.getClass() + "Exception Message: " + e.getMessage() + "Event: " +event);
            }

            event.setHeaders(headers);
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events)
    {

        for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();)
        {
            Event next = intercept(iterator.next());
            if (next == null)
            {
                iterator.remove();
            }
        }

        return events;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder
    {
        @Override
        public void configure(Context context) {

            String tempDefaultValue = null;

            Map<String, String> contextMap = context.getParameters();
            for (String key : contextMap.keySet()) {
                switch (key) {
                    case "json.addHeader":
                        header = context.getString(key);
                        break;
                    case "json.addHeader.defaultValue":
                        if(header != null) {
                            defaultValue = context.getString(key);
                        }
                        else {
                            tempDefaultValue = context.getString(key); }
                        break;
                    case "json.addHeader.forKafka":
                        defaultFieldKey = context.getString(key);
                        break;
                    case "json.timestamp":
                        timeStampField = context.getString(key);
                        break;
                }
            }
            if(tempDefaultValue != null){
                if( header != null){
                    defaultValue = tempDefaultValue;
                }
                else {
                    logger.warning("json.addHeader.defaultValue "
                            + tempDefaultValue + "will be ignored");
                }
            }
        }

        @Override
        public Interceptor build() {
            return new JsonCustomInterceptor();
        }
    }


}