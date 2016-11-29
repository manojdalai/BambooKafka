package com.ascend.connect.sqlserver;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class SQLServerSinkConnector extends SinkConnector
{
	//This class overrides the life-cycle methods needed by KafkaConnect framework
	
	private static final Logger log=LoggerFactory.getLogger("SQLServerConnectLogger");
		
    public static final String CONNECTION_URL_CONFIG = "connection.url";
    public static final String TABLE_NAMES_CONFIG = "table.names";
    public static final String PRIMARY_KEYS_CONFIG = "table.primarykeys";
    public static final String BATCH_SIZE = "batch.size";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, "JDBC connection URL for the database")
    .define(TABLE_NAMES_CONFIG, Type.STRING, Importance.HIGH, "Target table names")
    .define(PRIMARY_KEYS_CONFIG, Type.STRING, Importance.HIGH, "Target table primary keys")
    .define(BATCH_SIZE, Type.STRING, Importance.HIGH, "Batch Size");
    
    private String connectionURL;
    private String tableNames;
    private String primaryKeys;
    private String batchSize;
    private String topics;
    
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
    
   //Load configuration from properties file
    @Override
    public void start(Map<String, String> props) throws ConfigException {
    	connectionURL= props.get(CONNECTION_URL_CONFIG);
    	tableNames=props.get(TABLE_NAMES_CONFIG);
    	primaryKeys=props.get(PRIMARY_KEYS_CONFIG);
    	batchSize=props.get(BATCH_SIZE);
        if (connectionURL == null) throw new ConfigException ("Configuration Error: Connection URL must be specified");
        if (primaryKeys == null) throw new ConfigException ("Configuration Error: Primary keys must be specified");
        if (tableNames == null) throw new ConfigException ("Configuration Error: Target table names must be specified");
        if (batchSize == null) {
        	batchSize="1";
        	log.info("Batch size not specified in the config file; setting it to 1");
        }
        topics=props.get(TOPICS_CONFIG);
        
        log.info("Configuration loaded successfully");
    }
    
   //Set the task class
    @Override
    public Class<? extends Task> taskClass() {
      return SQLServerSinkTask.class;
    }
    
    // Return an appropriate number of tasks to the KafkaConnect framework
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<String, String>();
                config.put(CONNECTION_URL_CONFIG, connectionURL);
                config.put(PRIMARY_KEYS_CONFIG,primaryKeys);
                config.put(BATCH_SIZE,batchSize);
                config.put(TOPICS_CONFIG,topics);
                config.put(TABLE_NAMES_CONFIG, tableNames);
                
                configs.add(config);
        }
        return configs;
       }
    
    //There is nothing to stop at this point
    @Override
    public void stop() {
    	}
 
    
}
