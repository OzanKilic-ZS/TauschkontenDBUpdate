package org.example;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class ZSTransactionUpdate {

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

        String sql = "SELECT transactionId, customer2CaseAndTypeId, lieferAbholdatum, lieferscheinNr, " +
                "abholscheinNr, auftragsNrZs, auftragsNrKunde, rechnungsNrZS, buchungsinfo, lieferungZS, abholungZS, saldo, " +
                "creationDate, createdBy, saldenbestaetigungsDatum, saldenbestaetigungsPerson, bemerkung FROM ZSTransaction";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, AttributeValue> item = new HashMap<>();

                putItem(item, rs, "transactionId", "STRING");
                putItem(item, rs, "customer2CaseAndTypeId", "STRING");
                putItem(item, rs, "lieferAbholdatum", "STRING");
                putItem(item, rs, "lieferscheinNr", "STRING");
                putItem(item, rs, "abholscheinNr", "STRING");
                putItem(item, rs, "auftragsNrZs", "STRING");
                putItem(item, rs, "auftragsNrKunde", "STRING");
                putItem(item, rs, "rechnungsNrZS", "STRING");
                putItem(item, rs, "buchungsinfo", "STRING");
                putItem(item, rs, "lieferungZS", "INTEGER");
                putItem(item, rs, "abholungZS", "INTEGER");
                putItem(item, rs, "saldo", "INTEGER");
                putItem(item, rs, "creationDate", "STRING");
                putItem(item, rs, "createdBy", "STRING");
                putItem(item, rs, "saldenbestaetigungsDatum", "STRING");
                putItem(item, rs, "saldenbestaetigungsPerson", "STRING");
                putItem(item, rs, "bemerkung", "STRING");

                PutItemRequest request = PutItemRequest.builder()
                        .tableName("ZSTransaction")
                        .item(item)
                        .build();

                dynamoDb.putItem(request);

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        dynamoDb.close();
    }

    public static void putItem(Map<String, AttributeValue> item, ResultSet rs, String col, String type) throws SQLException {
        String val = rs.getString(col);
        if (val == null || val.isEmpty()) return;

        switch (type) {
            case "STRING":
                item.put(col, AttributeValue.builder().s(val).build());
                break;
            case "INTEGER":
                item.put(col, AttributeValue.builder().n(val).build());
                break;
        }
    }
}