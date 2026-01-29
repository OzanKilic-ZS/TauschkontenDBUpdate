package org.example;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.sql.Statement;
import java.sql.ResultSet;

public class CustomerUpdate {

    public static void main(String[] args) {
        select();
    }

    public static Connection connect() {
        String url = "jdbc:sqlite:C:\\java\\tauschkonten\\tauschkonten-backend\\tausch.db"; // Pfad zur DB-Datei

        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println("Fehler bei der Verbindung");
            e.printStackTrace();
            return null;
        }
    }

    public static void select() {
        DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(software.amazon.awssdk.regions.Region.EU_CENTRAL_1)
                .build();

        String sql = "SELECT customerId, customerNr, name, strasse, plz, ort, land FROM customer";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("pk", AttributeValue.builder().s("Customer").build());
                item.put("sk", AttributeValue.builder().s("customer#" + rs.getString("customerId")).build());
                item.put("customerNr", AttributeValue.builder().s(rs.getString("customerNr")).build());
                item.put("name", AttributeValue.builder().s(rs.getString("name")).build());
                item.put("strasse", AttributeValue.builder().s(rs.getString("strasse")).build());
                item.put("plz", AttributeValue.builder().s(rs.getString("plz")).build());
                item.put("ort", AttributeValue.builder().s(rs.getString("ort")).build());
                item.put("land", AttributeValue.builder().s(rs.getString("land")).build());

                PutItemRequest request = PutItemRequest.builder()
                        .tableName("Customer2CaseAndType")
                        .item(item)
                        .build();

                dynamoDb.putItem(request);

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        dynamoDb.close();
    }
}