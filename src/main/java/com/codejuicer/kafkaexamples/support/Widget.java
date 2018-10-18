package com.codejuicer.kafkaexamples.support;

/**
 * A worthless object used for demonstrating customer serializers.
 * So I suppose it isn't entirely worthless.
 */
public class Widget {
    private String widgetName;
    private String widgetDescription;
    private int widgetId;
    
    public Widget(int widgetId, String widgetName, String widgetDescription) throws Exception {
        if(null == widgetName) throw new Exception("Widget name cannot be null.");
        if(null == widgetDescription) throw new Exception("Widget description cannot be null.");
        
        this.widgetName = widgetName;
        this.widgetId = widgetId;
        this.widgetDescription = widgetDescription;
    }

    public String getWidgetName() {
        return widgetName;
    }

    public int getWidgetId() {
        return widgetId;
    }
    
    public String getWidgetDescription() {
        return widgetDescription;
    }
}
