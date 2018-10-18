package com.codejuicer.kafkaexamples.serializers;

import com.codejuicer.kafkaexamples.support.Widget;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class WidgetSerializer implements Serializer<Widget> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // Intentionally left empty.
    }

    @Override
    public byte[] serialize(String s, Widget widget) {
        // No need to process a null widget.
        if(null == widget) return null;
        
        byte[] widgetNameSerialized;
        byte[] widgetDescriptionSerialized;
        
        try {
            // In most cases it would be good to check for null strings. But Widget enforces "not null" on instantiation.
            widgetNameSerialized = widget.getWidgetName().getBytes("UTF-8");
            widgetDescriptionSerialized = widget.getWidgetDescription().getBytes("UTF-8");

            // Allocate bytes for ID (int) + name length (int) + widget name + description length (int) + widget description.
            ByteBuffer serialized = ByteBuffer.allocate(Integer.BYTES*3 + widgetNameSerialized.length + widgetDescriptionSerialized.length);
            serialized.putInt(widget.getWidgetId());
            serialized.putInt(widgetNameSerialized.length);
            serialized.put(widgetNameSerialized);
            serialized.putInt(widgetDescriptionSerialized.length);
            serialized.put(widgetDescriptionSerialized);
            
            return serialized.array();
        } catch (Exception exc) {
            throw new SerializationException("Error serializing the widget: " + exc);
        }
    }

    @Override
    public void close() {
        // Nope, nothing to see here.
    }
}
