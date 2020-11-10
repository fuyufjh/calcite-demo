package me.ericfu.calcite;

import org.apache.calcite.adapter.tpch.TpchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TpchSimpleDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");

        final String sql = TpchQuery.Q9;

        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");
        info.setProperty("materializationsEnabled", "false");

        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();

            TpchSchema tpchSchema = new TpchSchema(0.1, 1, 1, true);
            rootSchema.add("tpch", tpchSchema);
            calciteConnection.setSchema("tpch");

            try (Statement statement = calciteConnection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    printResultSet(System.out, resultSet);
                }
            }
        }
    }

    private static void printResultSet(PrintStream out, ResultSet rs) throws SQLException {
        printColumnLabels(out, rs.getMetaData());
        while (rs.next()) {
            printColumnLabels(out, rs);
        }
    }

    private static void printColumnLabels(PrintStream out, ResultSetMetaData rsmd) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rsmd.getColumnCount(); i++) {
            String column = rsmd.getColumnLabel(i);
            sb.append(column).append("\t");
        }
        out.println(sb.toString());
    }

    private static void printColumnLabels(PrintStream out, ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            Object val = rs.getObject(i);
            sb.append(val).append("\t");
        }
        out.println(sb.toString());
    }
}
