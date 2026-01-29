package org.example;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class ZSTransactionUpdate {
    static DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    static DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
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

        String sql = "SELECT transactionId, tr.customer2CaseAndTypeId, lieferAbholdatum, lieferscheinNr, " +
                // "abholscheinNr, auftragsNrZs, auftragsNrKunde, rechnungsNrZS, buchungsinfo, lieferungZS, abholungZS, saldo, " +
                "abholscheinNr, auftragsNrZs, auftragsNrKunde, rechnungsNrZS, buchungsinfo, lieferungZS, abholungZS, " +
                "creationDate, createdBy, saldenbestaetigungsDatum, saldenbestaetigungsPerson, bemerkung , custName, custNr, custCaseTypeBeschreibung " +
                " from ZSTransaction tr, CustomerCaseAndType_V cct_v " +
                " where tr.customer2CaseAndTypeId = cct_v.customer2CaseAndTypeId "
               // + " and tr.customer2CaseAndTypeId='9e9f4c55-5721-46dd-9d96-82ee5c957357' "
                ;

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, AttributeValue> item = new HashMap<>();

                putItem(item, rs, "transactionId", "STRING");
                putItem(item, rs, "customer2CaseAndTypeId", "STRING");
                putItem(item, rs, "lieferAbholdatum", "DATE");
                putItem(item, rs, "lieferscheinNr", "STRING");
                putItem(item, rs, "abholscheinNr", "STRING");
                putItem(item, rs, "auftragsNrZs", "STRING");
                putItem(item, rs, "auftragsNrKunde", "STRING");
                putItem(item, rs, "rechnungsNrZS", "STRING");
                putItem(item, rs, "buchungsinfo", "STRING");
                putItem(item, rs, "lieferungZS", "INTEGER");
                putItem(item, rs, "abholungZS", "INTEGER");
                //putItem(item, rs, "saldo", "INTEGER");
                putItem(item, rs, "creationDate", "STRING");
                putItem(item, rs, "createdBy", "STRING");
                putItem(item, rs, "saldenbestaetigungsDatum", "STRING");
                putItem(item, rs, "saldenbestaetigungsPerson", "STRING");
                putItem(item, rs, "bemerkung", "STRING");
                putItem(item, rs, "bemerkung", "STRING");
                putItem(item, rs, "custName", "STRING");
                putItem(item, rs, "custNr", "INTEGER");
                putItem(item, rs, "custCaseTypeBeschreibung", "STRING");

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
            case "DATE":
                LocalDate date = LocalDate.parse(val, inputFormat);
                item.put(col, AttributeValue.builder().s(date.format(outputFormat)).build());
                break;
            case "STRING":
                item.put(col, AttributeValue.builder().s(val).build());
                break;
            case "INTEGER":
                item.put(col, AttributeValue.builder().n(val).build());
                break;
        }
    }
}