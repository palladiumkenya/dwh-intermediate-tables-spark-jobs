package org.kenyahmis.lastvisitasat;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadLastVisitAsAt {
    private static final Logger logger = LoggerFactory.getLogger(LoadLastVisitAsAt.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Latest Visit As At table");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        Connection conn = null;
        try {
            String dbURL = rtConfig.get("spark.ods.url");
            String user = rtConfig.get("spark.ods.user");
            String pass = rtConfig.get("spark.ods.password");
            conn = DriverManager.getConnection(dbURL, user, pass);
            if (conn != null) {
                String query = new LoadLastVisitAsAt().loadQuery();
                Statement statement = conn.createStatement();
                statement.setQueryTimeout(7200);
                statement.execute(query);
                logger.info("Loaded Latest Visit As At table");
            }else {
                throw new RuntimeException("Failed to connect to sql database");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Failed to run Latest Visit As At  query");
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                logger.error("Failed to close sql connection");
                ex.printStackTrace();
            }
        }
    }

    private String loadQuery() {
        String query;
        final String fileName = "LastVisitAsAt.sql";
        InputStream inputStream = LoadLastVisitAsAt.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            logger.error(fileName + " not found");
            throw new RuntimeException(fileName + " not found");
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            throw new RuntimeException("Failed to load query from file");
        }
        return query;
    }
}
