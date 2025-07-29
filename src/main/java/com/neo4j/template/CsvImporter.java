package com.neo4j.template;

import org.neo4j.driver.*;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.Neo4jException;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

public class CsvImporter {

    private static final String DUCKDB_JDBC_URL = "jdbc:duckdb:";
    public final String database;
    public final Driver neo4jDriver;
    public final SessionConfig sessionConfig;

    public CsvImporter(String database, Driver neo4jDriver) {
        this.database = database;
        this.neo4jDriver = neo4jDriver;
        this.sessionConfig = SessionConfig.builder().withDatabase(database).build();
    }

    @FunctionalInterface
    public interface DataTransformer {
        List<Map<String, Object>> transform(List<Map<String, Object>> chunk);
    }

    private void sendChunkToNeo4j(Session session, String cypherQuery, List<Map<String, Object>> chunk) {
        try {
            session.executeWrite(tx -> {
                tx.run(cypherQuery, Collections.singletonMap("rows", chunk)).consume();
                return null;
            });
            System.out.print(".");
        } catch (Neo4jException e) {
            System.err.println("Error sending data to Neo4j: " + e.getMessage());
        }
    }

    public void readCsv(Driver neo4jDriver, String sqlQuery, String cypherQuery) {
        readCsv(neo4jDriver, sqlQuery, cypherQuery, chunk -> chunk, 10000);
    }

    public void readCsv(Driver neo4jDriver, String sqlQuery, String cypherQuery,
                               DataTransformer transformer, int batchSize) {
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(DUCKDB_JDBC_URL);
             Statement stmt = conn.createStatement()) {

            // Optional: Enable DuckDB extensions if needed (e.g., Excel)
//            stmt.execute("INSTALL excel");
//            stmt.execute("LOAD excel");

            try (ResultSet rs = stmt.executeQuery(sqlQuery)) {
                List<Map<String, Object>> buffer = new ArrayList<>();
                ResultSetMetaData meta = rs.getMetaData();
                int rowCount = 0;

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        row.put(meta.getColumnLabel(i), rs.getObject(i));
                    }
                    buffer.add(row);
                    rowCount++;

                    if (rowCount % batchSize == 0) {
                        List<Map<String, Object>> chunk = transformer.transform(buffer);
                        try (Session session = neo4jDriver.session()) {
                            sendChunkToNeo4j(session, cypherQuery, chunk);
                        }
                        buffer.clear();
                    }
                }

                // Send any remaining rows
                if (!buffer.isEmpty()) {
                    List<Map<String, Object>> chunk = transformer.transform(buffer);
                    try (Session session = neo4jDriver.session(sessionConfig)) {
                        sendChunkToNeo4j(session, cypherQuery, chunk);
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("\nDone in " + (endTime - startTime) / 1000.0 + " sec");
    }

}
