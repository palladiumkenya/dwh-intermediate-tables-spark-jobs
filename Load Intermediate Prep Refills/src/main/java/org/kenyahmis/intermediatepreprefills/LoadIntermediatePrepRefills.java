package org.kenyahmis.intermediatepreprefills;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadIntermediatePrepRefills {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            String dbURL = System.getenv("ODS_HOST_URL");
            String user = System.getenv("ODS_USER");
            String pass = System.getenv("ODS_PASSWORD");
            String timeout = System.getenv("ODS_QUERY_TIMEOUT");
            conn = DriverManager.getConnection(dbURL, user, pass);
            if (conn != null) {
                String query = new LoadIntermediatePrepRefills().loadQuery();
                Statement statement = conn.createStatement();
                statement.setQueryTimeout(Integer.parseInt(timeout));
                statement.execute(query);
                System.out.println("Loaded intermediate prep refills table");
            } else {
                throw new RuntimeException("Failed to connect to sql database");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Failed to run intermediate table query");
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.err.println("Failed to close sql connection");
                ex.printStackTrace();
            }
        }
    }

    private String loadQuery() {
        String query;
        final String fileName = "LoadIntermediatePrepRefills.sql";
        InputStream inputStream = LoadIntermediatePrepRefills.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            System.err.println(fileName + " not found");
            throw new RuntimeException(fileName + " not found");
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load query from file");
        }
        return query;
    }
}
