package org.example;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.sql.Statement;
import java.sql.ResultSet;

public class Customer2CaseAndTypeV {

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

        String sql = "select customerCaseId, customerId, customerCaseTypeId, customer2CaseAndTypeId, " +
                "custName, custStrasse, custPLZ, custOrt, custLand, " +
                "custCaseName, custCaseTypeName, custCaseTypeBeschreibung, Notizen from CustomerCaseAndType_V";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, AttributeValue> item = new HashMap<>();
                String pk = "case#" + rs.getString("customerCaseId");
                String sk = "customer#" + rs.getString("customerId") + "#caseType#" + rs.getString("customerCaseTypeId");

                item.put("pk", AttributeValue.builder().s(pk).build());
                item.put("sk", AttributeValue.builder().s(sk).build());
                item.put("id", AttributeValue.builder().s(rs.getString("customerId")).build());
                item.put("customer2CaseAndTypeId", AttributeValue.builder().s(rs.getString("customer2CaseAndTypeId")).build());
                item.put("name", AttributeValue.builder().s(rs.getString("custName")).build());
                item.put("strasse", AttributeValue.builder().s(rs.getString("custStrasse")).build());
                item.put("ort", AttributeValue.builder().s(rs.getString("custOrt")).build());
                item.put("plz", AttributeValue.builder().s(rs.getString("custPLZ")).build());
                item.put("land", AttributeValue.builder().s(rs.getString("custLand")).build());
                item.put("caseTypeName", AttributeValue.builder().s(rs.getString("custCaseTypeName")).build());
                item.put("caseTypeBeschreibung", AttributeValue.builder().s(rs.getString("custCaseTypeBeschreibung")).build());
                item.put("note", AttributeValue.builder().s(rs.getString("notizen")).build());
                item.put("noteLastChanged", AttributeValue.builder().s(LocalDateTime.now().toString()).build());
                item.put("noteChangedBy", AttributeValue.builder().s("technik@zs-paletten.de").build());

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