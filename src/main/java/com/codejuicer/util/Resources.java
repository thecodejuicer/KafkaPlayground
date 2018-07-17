package com.codejuicer.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public class Resources {
    
    private HashMap<String, Object> configStore;
    
    private static Resources instance;
    public static Logger logger = LoggerFactory.getLogger("com.codejuicer.kafkaexamples");
    
    public static final String KAFKA_BOOTSTRAP_SERVER = "kafka.bootstrapserver";
    
    private Resources(){
        configStore = new HashMap<>();
        logger.trace("Resources object instantiated.");
    }

    /**
     * Load important configuration files.
     */
    private static synchronized void loadConfigs() {
        logger.trace("Loading configuration files.");
        instance.configStore.put(KAFKA_BOOTSTRAP_SERVER,"localhost:9092");
    }
    
    public static synchronized Resources getInstance() {
        if(null == instance) {
            
            instance = new Resources();
            loadConfigs();
        }
        
        return instance;
    }
    
    public final Object getConfig(final String configName) {
        return instance.configStore.get(configName);
    }
}
