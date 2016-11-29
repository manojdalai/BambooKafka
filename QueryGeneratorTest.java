package io.ascend.connect.sqlserver;

import com.ascend.connect.sqlserver.Query;

import java.io.IOException;
import java.sql.*;

/**
 * Created by ravi on 13/9/16.
 */
public class QueryGeneratorTest {

    public  String dbConnPath;

    //"connection.url":"jdbc:sqlserver://10.200.128.29:1433;databaseName=demo;user=connect_user;password=Lemon123",

    public String sqlURI() {

        dbConnPath="10.200.128.29:1433;databaseName=demo;user=connect_user;password=Lemon123";
        System.out.println(dbConnPath);
        return "jdbc:sqlserver://" + dbConnPath;

    }

    private String currentTableName;
    private String previousTableName;
    private String connectionURL;
    private String tableNames;

    private Connection connection;
    private PreparedStatement stmt;
    private Query qry;
    private String qryString;
    private int recordsUpdated;


    public void setUp() throws IOException, SQLException {
        connection = DriverManager.getConnection(sqlURI());
        connection.setAutoCommit(false);
    }


    public void tearDown() throws IOException, SQLException {
        connection.close();
    }

    public void createTable(final String createSql) throws SQLException {
        execute(createSql);
    }
    public void execute(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            connection.commit();
        }
    }
    public void deleteTable(final String table) throws SQLException {
        execute("DROP TABLE IF EXISTS " + table);
        //random errors of table not being available happens in the unit tests
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean doesTableExist(final Connection connection, final String tableName) throws SQLException {
        final String catalog = connection.getCatalog();
        final DatabaseMetaData meta = connection.getMetaData();
        final String product = meta.getDatabaseProductName();
        final String schema = getSchema(connection, product);
        try (ResultSet rs = meta.getTables(catalog, schema, tableName, new String[]{"TABLE"})) {
            final boolean exists = rs.next();
            return exists;
        }
    }

    private static String getSchema(final Connection connection, final String product) throws SQLException {
        if (product.toLowerCase().startsWith("sqlserver")) {
            return connection.getSchema();
        } else {
            return null;
        }
    }


    public void tableExists() throws SQLException {
      }

    public interface ResultSetReadCallback {
        void read(final ResultSet rs) throws SQLException;
    }

    public int select(final String query, final QueryGeneratorTest.ResultSetReadCallback callback) throws SQLException {
        int count = 0;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    callback.read(rs);
                    count++;
                }
            }
        }
        return count;
    }


    public void getBaseQueryTest() throws Exception {

    }

}
