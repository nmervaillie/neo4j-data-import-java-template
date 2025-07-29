package com.neo4j.template;

import io.github.cdimascio.dotenv.Dotenv;
import org.neo4j.driver.*;

import java.util.Map;

public class Runner {

    void run() {

        Dotenv dotenv = Dotenv.load();
        String uri = dotenv.get("NEO4J_URI");
        String username = dotenv.get("NEO4J_USERNAME");
        String password = dotenv.get("NEO4J_PASSWORD");
        String database = dotenv.get("NEO4J_DATABASE");

        Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));

        driver.executableQuery("CREATE DATABASE $database IF NOT EXISTS WAIT")
                .withConfig(QueryConfig.builder().withDatabase("system").build())
                .withParameters(Map.of("database", database))
                .execute();

        // delete existing data
        try (Session session = driver.session(SessionConfig.builder().withDatabase(database).build())) {
            session.run("MATCH (n:!`_Bloom_Perspective_`&!`_Bloom_Scene_`&!`_Neodash_Dashboard`) CALL(n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 rows").consume();
        }
        // create indexes
        try (Session session = driver.session(SessionConfig.builder().withDatabase(database).build())) {
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Category) REQUIRE n.id IS UNIQUE").consume();
        }

        CsvImporter csvImporter = new CsvImporter(database, driver);
        csvImporter.readCsv(driver,
                """
                SELECT categoryID as id, categoryName, description
                FROM read_csv('src/test/resources/data/categories.csv')
                """,
                """
                UNWIND $rows as row
                MERGE (cat:Category {id:row.id}) SET cat += row
                """);
    }

    public static void main(String[] args) {
        new Runner().run();
    }
}
