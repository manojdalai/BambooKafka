package com.ascend.connect.sqlserver;


import com.ascend.connect.sqlserver.utils.ArrayUtilities;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

//import com.javaunderground.jdbc.*;


public class SQLServerSinkTask extends SinkTask{
	
		
	private static final Logger log=LoggerFactory.getLogger("SQLServerConnectLogger");

	private String currentTableName;
	private String previousTableName;
    private String connectionURL;
    private String tableNames;
    private String primaryKeys;
    private String currentPrimaryKey;
    private Connection dbConn;
    private PreparedStatement stmt;
    private Query qry;
    private String qryString;
    private int recordsUpdated;
    private String previousOperation;
    private String currentOperation;
    private int batchSize;
    private String topics;
    private String currentTopicName;

	// added for error topic alerting scenario
	private Map<String, String> config;
	private static String HEADER_KAFKA_REST_CONTENT_TYPE = "application/vnd.kafka.json.v1+json";
	private static String HEADER_KAFKA_REST_ACCEPT = "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json";


	@Override
	    public void start(Map<String, String> props) {
    	tableNames = props.get(SQLServerSinkConnector.TABLE_NAMES_CONFIG);
    	connectionURL= props.get(SQLServerSinkConnector.CONNECTION_URL_CONFIG);
		primaryKeys= props.get(SQLServerSinkConnector.PRIMARY_KEYS_CONFIG);
		batchSize=Integer.parseInt(props.get(SQLServerSinkConnector.BATCH_SIZE));
		topics = props.get(SQLServerSinkConnector.TOPICS_CONFIG);


		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			dbConn= DriverManager.getConnection(connectionURL);
			dbConn.setAutoCommit(false);
			log.info("Succesfully created a connection to the database");
			
		} catch (ClassNotFoundException e) {
			sendErrorMsgToRestProxy(e.getMessage(),"Driver class not loaded"  + "-"+String.valueOf(System.currentTimeMillis()));
			log.error("Driver class not loaded" +  e.getMessage());
			throw new ConnectException(e);			
		}
    	catch (SQLException e) {
			sendErrorMsgToRestProxy(e.getMessage(),"Unable to create a connection"  + "-"+String.valueOf(System.currentTimeMillis()));
			log.error("Unable to create a connection" +  e.getMessage());
    		throw new ConnectException(e);    		
		}

	}

	 
	    @Override
	    public void put(Collection<SinkRecord> sinkRecords) throws ConnectException{
	    	log.debug("Start of put method" );
	    	try {
	    	 int batchCount = sinkRecords.size();
	    	 int counter = 0;
	    	 int batchCounter=0;
	    	 previousOperation=null;
	    	 previousTableName=null;
	    	 stmt=null;
	    	// DebugLevel debug = DebugLevel.ON;
	    	 log.debug("Number of records in this batch is " + batchCount);
	      	
	    	for (SinkRecord rec : sinkRecords) {
	    	
	    	counter++;	
	    	currentTopicName= rec.topic(); //determine the topic for this message
	    	log.debug("Topic name is  " + currentTopicName );
	    	
	    	//determine table-name (from configuration) based on the topic this message came from
	    	currentTableName = (tableNames.split(",")[java.util.Arrays.asList(topics.split(",")).indexOf(currentTopicName)]);
	    	
	    	
	    	log.debug("Tablename is  " + currentTableName );
	    		
	    	//determine primary key (from configuration) based on table-name
	    	currentPrimaryKey=(primaryKeys.split(",")[java.util.Arrays.asList(tableNames.split(",")).indexOf(currentTableName)]);
	    	log.debug("Primary key  is  " + currentPrimaryKey );
	    	
	        QueryGenerator qryGenerator = new QueryGenerator(rec);
	        qry = qryGenerator.getBaseQuery(currentTableName,currentPrimaryKey);
	        qryString = qry.getQuery();
	        currentOperation = qry.getOperation();
	         
	         log.debug("Previous operation is " + previousOperation );
	         log.debug("Current operation is " + currentOperation );
	         log.debug("Previous table name is " + previousTableName );
	         log.debug("Current table name is " + currentTableName );
	         
	         if(previousOperation == null || !(previousOperation.equals(currentOperation)) || batchCounter == batchSize || !(previousTableName.equals(currentTableName)))
	         {
	        	 log.debug("Inside 1 ");
	        		if (stmt != null){
		        		log.debug("Executing previous statement");
		        		recordsUpdated=ArrayUtilities.addElements(stmt.executeBatch());
		        		batchCounter=0;
		        		log.debug("Number of records updated in target database is : " + recordsUpdated);   		
		        	}
		        	log.debug("Query string is " + qryString );
	        		stmt = dbConn.prepareStatement(qryString);
		        	//stmt=StatementFactory.getStatement(dbConn,qryString,debug);
		        	qryGenerator.setQueryParams(stmt,currentPrimaryKey,qryString);
		        	stmt.addBatch();
		        	batchCounter++;
	 	        	log.debug("Added a record to batch from inside 1");
		    }
	         else  
	         {
	        	     log.debug("Inside 2 ");
	        	     qryGenerator.setQueryParams(stmt,currentPrimaryKey,qryString);
	 	        	  stmt.addBatch();
	 	        	  batchCounter++;
	 	        	  log.debug("Added a record to batch from inside 2");
			 }
	         
	        if(counter==batchCount)
	         {
	        	 	 log.debug("Inside 3 ");        
	        		 recordsUpdated=ArrayUtilities.addElements(stmt.executeBatch());
	        		 dbConn.commit();
	        		 log.debug("Number of records updated in target database is : " + recordsUpdated);
	        }
	         
	         previousOperation=currentOperation;
	         previousTableName=currentTableName;
     	                        
	        }
	    }
	    	catch (SQLException e) {
				sendErrorMsgToRestProxy(e.getMessage(),"Error in creating/executing a statement or adding record to batch"  + "-"+String.valueOf(System.currentTimeMillis()));
				log.error("Error in creating/executing a statement or adding record to batch: " +  e.getMessage());
	    		throw new ConnectException(e);
			}
	    	catch (Exception e) {
				sendErrorMsgToRestProxy(e.getMessage(),"Error in creating/executing a statement or adding record to batch"  + "-"+String.valueOf(System.currentTimeMillis()));
				log.error("Error in creating/executing a statement or adding record to batch: " +  e.getMessage());
	    		throw new ConnectException(e); 
			}
	    	log.debug("End of put method" );
}
	 	 
	 @Override
	    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		 //nothing to flush here
	    }
	 
	 @Override
	  public void stop(){
		 
		// Close DB connection
		 try {
			 dbConn.close();
			
		} catch (SQLException e) {
			 sendErrorMsgToRestProxy(e.getMessage(),"Unable to close the db connection"  + "-"+String.valueOf(System.currentTimeMillis()));
			 log.error("Unable to close the db connection" +  e.getMessage());
    	}
		 finally{
			 try{
		         if(stmt!=null)
		            dbConn.close();
		      }catch(SQLException se){
				 sendErrorMsgToRestProxy(se.getMessage(),"Unable to close the db connection"  + "-"+String.valueOf(System.currentTimeMillis()));
				 log.error("Unable to close the db connection" +  se.getMessage());
		      }
		      try{
		         if(dbConn!=null)
		        	 dbConn.close();
		      }catch(SQLException se){
				  sendErrorMsgToRestProxy(se.getMessage(),"Unable to close the db connection"  + "-"+String.valueOf(System.currentTimeMillis()));
				  log.error("Unable to close the db connection" +  se.getMessage());
		     }
		 }
	 }

	 public String version(){
		 return new SQLServerSinkConnector().version();
	 }

	// error topic alerting
	public void sendErrorMsgToRestProxy(String errorMessage, String message) {
		String url = config.get("kafka.restProxyBaseUrl") + config.get("errorTopicName");
		log.debug("got the url :" + url);
		HttpClient client = HttpClientBuilder.create().build();
		HttpPost post = new HttpPost(url);
		post.setHeader(org.apache.http.HttpHeaders.CONTENT_TYPE, HEADER_KAFKA_REST_CONTENT_TYPE);
		post.setHeader(org.apache.http.HttpHeaders.ACCEPT, HEADER_KAFKA_REST_ACCEPT);
		org.apache.http.HttpResponse response=null;
		String jsonErrorString = "{\"records\": [{\"value\":" + "{\"error_message\":\"" + errorMessage + "\", \"message\":\"" + message + "\"}" + "}]}";
		log.debug("Publish error message -\n" + jsonErrorString + "\n");
		log.info("Publish error message -\n" + jsonErrorString + "\n");
		HttpEntity entity = null;
		try {
			entity = new ByteArrayEntity(jsonErrorString.getBytes("UTF-8"));
			post.setEntity(entity);
			response  = client.execute(post);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug("Response Code : " + response.getStatusLine().getStatusCode()+ " Response reason phrase : " + response.getStatusLine().getReasonPhrase());
	}
}
