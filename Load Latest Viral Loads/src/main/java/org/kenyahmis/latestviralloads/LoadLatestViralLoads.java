package org.kenyahmis.latestviralloads;

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

public class LoadLatestViralLoads {
    private static final Logger logger = LoggerFactory.getLogger(LoadLatestViralLoads.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load intermediate latest viral loads table");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        Connection conn = null;
        try {
            String dbURL = rtConfig.get("spark.sink.url");
            String user = rtConfig.get("spark.sink.user");
            String pass = rtConfig.get("spark.sink.password");
            String timeout = rtConfig.get("spark.intermediateQuery.timeout");
            conn = DriverManager.getConnection(dbURL, user, pass);
            if (conn != null) {
                String query = new LoadLatestViralLoads().loadQuery();
                Statement statement = conn.createStatement();
                statement.setQueryTimeout(Integer.parseInt(timeout));
                statement.execute(query);
                logger.info("Loaded intermediate latest viral loads table");
            }else {
                throw new RuntimeException("Failed to connect to sql database");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Failed to run intermediate latest viral loads query");
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
        final String fileName = "LatestViralLoads.sql";
        InputStream inputStream = LoadLatestViralLoads.class.getClassLoader().getResourceAsStream(fileName);
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
