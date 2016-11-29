package io.ascend.connect.sqlserver;


import com.ascend.connect.sqlserver.Query;
import com.ascend.connect.sqlserver.QueryGenerator;
import com.ascend.connect.sqlserver.SQLServerSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Assert;


import java.io.OutputStream;
import java.sql.*;
import java.util.*;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.mockito.Mockito.mock;

/**
 * Created by ravi on 29/9/16.
 */
public class SQLServerSinkTaskTest {

    Properties prop = new Properties();
    protected static Set<TopicPartition> assignment;
    OutputStream output = null;
    private Query query;
    private PreparedStatement stmt;
    private Connection dbConn;
    protected MockSinkTaskContext context;

    // Query statement if PrimaryKey is present
    String insertTemplate = "INSERT INTO CompleteInsights.CompleteInsightsLog (CompleteInstitutionAdditionsID,InstitutionID,Institution,IsEnabled,TableUpdated,tran_begin_time,operation) SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS ( SELECT 1 FROM CompleteInsights.CompleteInsightsLog WHERE CompleteInsightsLogID2 = ? )";
    String deleteTemplate = "DELETE FROM CompleteInsights.CompleteInsightsLog WHERE CompleteInsightsLogID = ?";
    String updateTemplate = "UPDATE CompleteInsights.CompleteInsightsLog SET CompleteInstitutionAdditionsID=?, InstitutionID=?, Institution=?, IsEnabled=?, TableUpdated=?, tran_begin_time=?, operation=? WHERE CompleteInsightsLogID = ?";

    // Query statement if CompositKey is present
    String compositeInsertTemplate = "INSERT INTO CompleteInsights.CompleteInsightsLog (CompleteInstitutionAdditionsID,InstitutionID,Institution,IsEnabled,TableUpdated,tran_begin_time,operation) SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS ( SELECT 1 FROM CompleteInsights.CompleteInsightsLog WHERE CompleteInsightsLogID = ? AND InstitutionID = ? )";
    String compositeDeleteTemplate = "DELETE FROM CompleteInsights.CompleteInsightsLog WHERE CompleteInsightsLogID = ? AND InstitutionID = ? ";
    String compositeUpdateTemplate = "UPDATE CompleteInsights.CompleteInsightsLog SET CompleteInstitutionAdditionsID=?, InstitutionID=?, Institution=?, IsEnabled=?, TableUpdated=?, tran_begin_time=?, operation=? WHERE CompleteInsightsLogID = ? AND InstitutionID = ? ";

    private static final Schema SCHEMA = SchemaBuilder.struct().name("com.ascendlearning.testTable")
            .field("CompleteInstitutionAdditionsID", Schema.OPTIONAL_INT32_SCHEMA)
            .field("InstitutionID", Schema.OPTIONAL_INT32_SCHEMA)
            .field("Institution", Schema.OPTIONAL_STRING_SCHEMA)
            .field("IsEnabled",Schema.OPTIONAL_INT32_SCHEMA)
            .field("TableUpdated",Schema.OPTIONAL_INT32_SCHEMA)
            .field("tran_begin_time",Schema.OPTIONAL_INT32_SCHEMA)
            .field("operation",Schema.OPTIONAL_INT32_SCHEMA)
           .build();

    QueryGeneratorTest queryGeneratorTest;

    protected Map<String, String> createProps() {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:sqlserver://10.200.128.34:1433;databaseName=ATICustomReporting;user=jdbcconnect;password=####");
        props.put("table.primarykeys", "CompleteInsightsLogID");
        props.put("batch.size", "1");
        props.put("errorTopicName", "errorTopicSql");
        props.put("kafka.restProxyBaseUrl", "http://10.200.128.209:8082/topics/");
        props.put("topics", "jdbc-view-CompleteInsights_CompleteInsightsLog_cdc_view");
        props.put("table.names", "CompleteInsights.CompleteInsightsLog");
        return props;
    }


    protected static class MockSinkTaskContext implements SinkTaskContext {

        private Map<TopicPartition, Long> offsets;
        private long timeoutMs;

        public MockSinkTaskContext() {
            this.offsets = new HashMap<>();
            this.timeoutMs = -1L;
        }
        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            this.offsets.putAll(offsets);
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
            offsets.put(tp, offset);
        }

        /**
         * Get offsets that the SinkTask has submitted to be reset. Used by the Copycat framework.
         * @return the map of offsets
         */
        public Map<TopicPartition, Long> offsets() {
            return offsets;
        }

        @Override
        public void timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        /**
         * Get the timeout in milliseconds set by SinkTasks. Used by the Copycat framework.
         * @return the backoff timeout in milliseconds.
         */
        public long timeout() {
            return timeoutMs;
        }

        /**
         * Get the timeout in milliseconds set by SinkTasks. Used by the Copycat framework.
         * @return the backoff timeout in milliseconds.
         */

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        @Override
        public void pause(TopicPartition... partitions) {
            return;
        }

        @Override
        public void resume(TopicPartition... partitions) {
            return;
        }
    }


    @Test
    public void getBaseQueryTestSinkTask() throws Exception {

     //   Map<String, String> props = createProps();
     //   SQLServerSinkTask task = new SQLServerSinkTask();
       // task.initialize(mock(SinkTaskContext.class));

      //  task.start(props);
 // starting the task for the mocked object
        final Struct sinkStructStructDelete = new Struct(SCHEMA)
                .put("CompleteInstitutionAdditionsID", 777)
                .put("InstitutionID", 2222)
                .put("Institution", "Test Institution")
                .put("IsEnabled",1)
                .put("TableUpdated",1321)
                .put("tran_begin_time",1321)
                .put("operation",1);

        final Struct sinkStructStructInsert = new Struct(SCHEMA)
                .put("CompleteInstitutionAdditionsID", 777)
                .put("InstitutionID", 2222)
                .put("Institution", "Test Institution")
                .put("IsEnabled",1)
                .put("TableUpdated",1321)
                .put("tran_begin_time",1321)
                .put("operation",2);

        final Struct sinkStructStructUpdate = new Struct(SCHEMA)
                .put("CompleteInstitutionAdditionsID", 777)
                .put("InstitutionID", 2222)
                .put("Institution", "Test Institution")
                .put("IsEnabled",1)
                .put("TableUpdated",1321)
                .put("tran_begin_time",1321)
                .put("operation",4);



        String key = "key";
        String topic ="jdbc-view-CompleteInsights_CompleteInsightsLog_cdc_view";
        Collection<SinkRecord> sinkRecords = new ArrayList<>();

        SinkRecord sinkRecord =
                new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, SCHEMA, sinkStructStructDelete, 1);

        System.out.println("sinkRecord :" + sinkRecord);
        Assert.assertNotNull(sinkRecord.key());
        sinkRecords.add(sinkRecord);

        SinkRecord sinkRecord1 =
                new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key,SCHEMA, sinkStructStructInsert, 2);
        sinkRecords.add(sinkRecord1);

        SinkRecord sinkRecord2 =
                new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, SCHEMA, sinkStructStructUpdate, 3);
        sinkRecords.add(sinkRecord2);

        SinkRecord sinkRecord3 =
                new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, SCHEMA, sinkStructStructDelete, 4);
        sinkRecords.add(sinkRecord3);

        //DELETE OPERATION
        QueryGenerator queryGeneratorDeleteOperation = new QueryGenerator(sinkRecord);
        query=  queryGeneratorDeleteOperation.getBaseQuery("CompleteInsights.CompleteInsightsLog","CompleteInsightsLogID:InstitutionID");
        System.out.println(queryGeneratorDeleteOperation.getBaseQuery("CompleteInsights.CompleteInsightsLog","CompleteInsightsLogID"));

        String queryStr = query.getQuery();
        String queryStrOperation = query.getOperation();
        System.out.println("printing query string :" + queryStr);
        System.out.println("printing query operation :" + queryStrOperation);
        System.out.println("queryStr1 - getPrimaryKey" + query.getPrimaryKey());
        if(query.getPrimaryKey().contains("AND")) {
            assertFalse(compositeDeleteTemplate,false);
            assertEquals(compositeDeleteTemplate,queryStr);
        }
        else {
            assertFalse(deleteTemplate,false);
            assertEquals(deleteTemplate,queryStr);
        }

        //INSERT OPERATION
        QueryGenerator queryGeneratorInsertOperation = new QueryGenerator(sinkRecord1);
        query =  queryGeneratorInsertOperation.getBaseQuery("CompleteInsights.CompleteInsightsLog","CompleteInsightsLogID:InstitutionID");
        System.out.println(queryGeneratorInsertOperation.getBaseQuery("CompleteInsights.CompleteInsightsLog","CompleteInsightsLogID:InstitutionID"));

        String queryStr1 = query.getQuery();
        String queryStrOperation1 = query.getOperation();
        System.out.println("printing query string sink record 1 :" + queryStr1);
        System.out.println("printing query operation sink record 1 :" + queryStrOperation1);
        System.out.println("queryStr1 - getPrimaryKey" + query.getPrimaryKey());
        if(query.getPrimaryKey().contains("AND")) {
            assertFalse(compositeInsertTemplate,false);
            assertEquals(compositeInsertTemplate,queryStr1);
        }
        else {
            assertFalse(insertTemplate,false);
            assertEquals(insertTemplate,queryStr1);
        }


        //UPDATE OPERATION
        QueryGenerator queryGeneratorUpdateOperation = new QueryGenerator(sinkRecord2);
        query =  queryGeneratorUpdateOperation.getBaseQuery("CompleteInsights.CompleteInsightsLog","CompleteInsightsLogID:InstitutionID");
        System.out.println(queryGeneratorUpdateOperation.getBaseQuery("CompleteInsights.CompleteInsightsLog","CompleteInsightsLogID:InstitutionID"));

        String queryStrdUpd = query.getQuery();
        String queryStrOperationUpd = query.getOperation();
        System.out.println("printing query string sink record 1 :" + queryStrdUpd);
        System.out.println("printing query operation sink record 1 :" + queryStrOperationUpd);
        System.out.println("queryStr1 - getPrimaryKey" + query.getPrimaryKey());
        if(query.getPrimaryKey().contains("AND")) {
            assertFalse(compositeUpdateTemplate,false);
            assertEquals(compositeUpdateTemplate,queryStrdUpd);
        }
        else {
            assertFalse(updateTemplate,false);
            assertEquals(updateTemplate,queryStrdUpd);
        }

        //  stmt = dbConn.prepareStatement(queryStr);

    }

}
