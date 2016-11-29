package com.ascend.connect.sqlserver;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class QueryGenerator {
	

	private static final Logger log=LoggerFactory.getLogger("SQLServerConnectLogger");
	private Schema recordSchema;
	private String query;
	private String operation;
	private List<Field> fields;
	private SinkRecord record;
	private Struct recordValue;
	public final String PRIMARYKEY_DELIMITER=":";
	private String primaryKey="";


	SQLServerSinkTask sqlSinkTask;

	public QueryGenerator(SinkRecord record){ //Constructor
		this.record =record;
		this.recordSchema=record.valueSchema();
    	this.fields = recordSchema.fields();
    	this.recordValue = (Struct)record.value();
    }
	
		
	public Query getBaseQuery(String tblName, String primKey) throws ConnectException{
		
		
		try {
	   	List<String> columnNames = new ArrayList<String>(); // list to store column names
    	String insertColumnsExpr;
    	String insertQueryParams;
    	Map<String,String> updateExprMap = new LinkedHashMap<String,String>(); //map to store update expression
    	    	
    	//loop over fields in each record, extract column names and add them to the list
    	for (Field fld : fields) 
    	{   
    		columnNames.add(fld.name());
    		if ((fld.name()).equals("operation")){ //determine the type of DML operation
    			operation= (recordValue).get(fld).toString();
    		}
    	
    	}
		//Prepare Composite_PrimaryKey OR PrimaryKey
		if(primKey.contains(PRIMARYKEY_DELIMITER)) {
			primaryKey =  primKey.replace(PRIMARYKEY_DELIMITER," = ? AND ").concat(" = ? ").toString();
		}
		else {
			primaryKey = primKey.concat(" = ? ").toString();
		}
		log.debug("Inside getBaseQuery() method. Preparing primary key in query paramerter format:- " + primaryKey);

			//create the query for this SinkRecord

			if (operation.equals("2")) { // this is an insert

				insertColumnsExpr = String.join(",", columnNames);
				insertQueryParams = String.join(",", new ArrayList<String>(Collections.nCopies(columnNames.size(), "?"))); //set parameters (?) equal to the number of columns
				//query = "INSERT INTO " + tblName + " (" + insertColumnsExpr +")" +  " VALUES (" + insertQueryParams + ")";

             /* QUERY format for INSERT IF NOT EXISTS
            insert into CompleteInsights.CompleteInsightsLog (CompleteInsightsLogID,TableName, RecordsCreated, UpdatedBy, UpdatedDate,tran_begin_time,operation)
            select 90,'TestDemoDataCreated',0,'SPROC',GETDATE(),GETDATE(),2 where not exists (select 1 from CompleteInsights.CompleteInsightsLog where CompleteInsightsLogID=90);
            */
				query = "INSERT INTO " + tblName + " (" + insertColumnsExpr + ")" + " SELECT " + insertQueryParams + " WHERE NOT EXISTS ( SELECT 1 FROM " + tblName + " WHERE " + primaryKey + ")";
			} else if (operation.equals("4")) // this is an update
			{
				for (int i = 0; i < columnNames.size(); i++) {
					updateExprMap.put(columnNames.get(i), "?");
				}

				query = "UPDATE " + tblName + " SET " + updateExprMap.toString().replace("{", "").replace("}", "") + " WHERE " + primaryKey;

			} else if (operation.equals("1")) // this is a delete
			{
				query = "DELETE FROM " + tblName + " WHERE " + primaryKey;

			} else {
				throw new ConnectException("This operation is not supported : " + operation);
			}

			log.debug("The query to run is " + query);

			//}
		}
		catch (Exception e){
			sqlSinkTask.sendErrorMsgToRestProxy(e.getMessage(),"Error in creating the base query"  + "-"+String.valueOf(System.currentTimeMillis()));
			log.error("Error in creating the base query: " + e.getMessage());
			throw new ConnectException(e);
		}
    	
    	return new Query(query,operation,primaryKey);
	
	}

 public void setQueryParams(PreparedStatement statement,String primKey, String queryString) {
	
	try {
		int indx;
		log.debug("primKey:- " + primKey);
		int size = primKey.split(PRIMARYKEY_DELIMITER).length;
		String[] primKeys = primKey.split(PRIMARYKEY_DELIMITER);
		String[] primaryKeyValue = new String[size];
		for(int i=0;i<primKeys.length;i++) {
			primaryKeyValue[i] = recordValue.get(primKeys[i]).toString();
			log.debug("primaryKeyValue"+i +"->" + primaryKeyValue[i]);
		}
		//String primaryKeyValue = recordValue.get(primKey).toString();
		//log.debug("PrimaryKeyValue:- " + primaryKeyValue);
		
		if (operation.equals("2") || operation.equals("4"))  // set parameter values only for inserts and updates
		{
			for (int i=0; i < fields.size(); i++){			
			
			Type fieldType = fields.get(i).schema().type();
			Object fieldValue= recordValue.get(fields.get(i));
			Date timestamp;
			Date date;
			Date time;
			
			
			log.debug("The schema for field name " + fields.get(i).name() + " is " + fieldType);
			log.debug("More details : " + fields.get(i).schema().toString());
			
									
				switch(fieldType) { //set each parameter based on its type
				
					case STRING: 
						statement.setString(i+1, (String)fieldValue);						
						break;
					case INT64:
						if (fields.get(i).schema().toString().contains("Timestamp")){ //Timestamp fields also show-up as  
							if (fieldValue != null)                                   // int64 so they need to be formatted
							{
								timestamp = (Date)fieldValue;             
								statement.setTimestamp(i+1,new java.sql.Timestamp(timestamp.getTime()));
							} else {
								statement.setTimestamp(i+1, null);
							}
							}
						else 
							{
							if (fieldValue != null) {
								statement.setLong(i+1, (Long)fieldValue);
							} else {
								statement.setNull(i+1, java.sql.Types.BIGINT);
							}
							}
						break;
					case INT32:
						if (fields.get(i).schema().toString().contains("Date")){ //Date fields also show-up as  
							if (fieldValue != null)                              // int32 so they need to be formatted
							{
								date = (Date)fieldValue;             
								statement.setDate(i+1,(new java.sql.Date(date.getTime())));
							} else {
								statement.setDate(i+1,null);
							}
							}
						else if (fields.get(i).schema().toString().contains("Time")){ //Time fields also show-up as  
							if (fieldValue != null)                                   // int32 so they need to be formatted
							{
								time = (Date)fieldValue;             
								statement.setTime(i+1,(new java.sql.Time(time.getTime())));
							} else {
								statement.setTime(i+1,null);
							}
							}
						else
							{
							if (fieldValue != null) {
								statement.setInt(i+1, (Integer)fieldValue);
							} else {
								statement.setNull(i+1,java.sql.Types.INTEGER);
							}
							}
						break;
					case INT16:
						if (fieldValue != null) {
							statement.setShort(i+1, (Short)fieldValue);
						} else {
							statement.setNull(i+1,java.sql.Types.SMALLINT);
						}
						break;
					case INT8:
						if (fieldValue != null) {
							statement.setByte(i+1, (Byte)fieldValue);
						} else {
							statement.setNull(i+1,java.sql.Types.BIT);
						}
						break;
					case BYTES:
						if (fields.get(i).schema().toString().contains("Decimal")){
							if (fieldValue != null) {
								statement.setBigDecimal(i+1, (BigDecimal)fieldValue);
							} else {
								statement.setBigDecimal(i+1,null);
							}
						}
						else
						{	
							if (fieldValue != null)		
								{
								java.nio.ByteBuffer byBuff= (java.nio.ByteBuffer)fieldValue;
								statement.setBytes(i+1,byBuff.array() );
								} else {
								statement.setBytes(i+1,null);
							}
						}
						break;
					case FLOAT64:
						if (fieldValue != null) {
							statement.setDouble(i+1, (Double)fieldValue);
						} else {
							statement.setNull(i+1,java.sql.Types.DOUBLE);
						}
						break;
					case FLOAT32:
						if (fieldValue != null) {
							statement.setFloat(i+1, (Float)fieldValue);
						} else {
							statement.setNull(i+1,java.sql.Types.FLOAT);
						}
						break;
					default:
						break;							
				} 
				
		}
	
		/*if (operation.equals("4"))
		{
			indx= StringUtils.countMatches(queryString,"?");
			statement.setString(indx,primaryKeyValue);
		}*/

            // SET Primary Key Value for INSERT & UPDATE Query
            indx = StringUtils.countMatches(queryString, "?");
			log.debug("Total parameterized index in insert or update operation-> " +indx );
			log.debug("primaryKeyValue count-> " + primaryKeyValue.length );
			int valueIndx = 0;
			for(int i = ((indx - primaryKeyValue.length)+1); i<= indx; i++ ){
				log.debug("index->"+i+" value->"+primaryKeyValue[valueIndx]);
				statement.setString(i, primaryKeyValue[valueIndx]);
				valueIndx++;
			}
			log.debug("index set done for insert or update operation");
	}
		// SET Primary Key Value for DELETE Query
		else
		{
			indx= StringUtils.countMatches(queryString, "?");
			log.debug("index count in delete operration is:- " + indx);
			for(int k = 0; k< primaryKeyValue.length; k++ ){
				statement.setString(k+1,primaryKeyValue[k]);
			}
			log.debug("index set done for delete operation");
		}

	}
	catch(SQLException e){
		sqlSinkTask.sendErrorMsgToRestProxy(e.getMessage(),"Error in setting query parameters"  + "-"+String.valueOf(System.currentTimeMillis()));
		log.error("Error in setting query parameters: " + e.getMessage());
			throw new ConnectException(e);
		}
	catch(Exception e){
		sqlSinkTask.sendErrorMsgToRestProxy(e.getMessage(),"Error in setting query parameters"  + "-"+String.valueOf(System.currentTimeMillis()));
		log.error("Error in setting query parameters: " + e.getMessage());
			throw new ConnectException(e);
		}

 }

}
