package com.codejuicer.kafkaexamples.deserializers;

import com.codejuicer.kafkaexamples.support.Widget;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class WidgetDeserializer implements Deserializer<Widget> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        
    }

    @Override
    public Widget deserialize(String topic, byte[] serializedWidget) {
        if(null == serializedWidget) return null;
        if(serializedWidget.length < Integer.BYTES*3) throw new SerializationException("Data size too small. Invalid widget.");
        
        int widgetId, widgetNameLength, widgetDescriptionLength;
        String widgetName, widgetDescription;
        
        // Parse all the bytes and construct a new Widget object.
        try {
            ByteBuffer widgetBuffer = ByteBuffer.wrap(serializedWidget);
            widgetId = widgetBuffer.getInt();
            
            // Get the length of the name
            widgetNameLength = widgetBuffer.getInt();
            // read the name
            byte[] widgetNameBytes = new byte[widgetNameLength];
            widgetBuffer.get(widgetNameBytes);
            widgetName = new String(widgetNameBytes, "UTF-8");
            
            // Get the length of the description
            widgetDescriptionLength = widgetBuffer.getInt();
            // read the description
            byte[] widgetDescriptionBytes = new byte[widgetDescriptionLength];
            widgetBuffer.get(widgetDescriptionBytes);
            widgetDescription = new String(widgetDescriptionBytes, "UTF-8");
            
            return new Widget(widgetId, widgetName, widgetDescription);
            
        } catch(Exception e) {
            throw new SerializationException("Couldn't deserialize the widget: " + e);
        }
    }

    @Override
    public void close() {

    }
}
