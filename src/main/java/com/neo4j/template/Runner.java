package com.neo4j.template;

import io.github.cdimascio.dotenv.Dotenv;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.QueryConfig;

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
