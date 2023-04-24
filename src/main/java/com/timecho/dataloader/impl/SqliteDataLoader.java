package com.timecho.dataloader.impl;

import com.timecho.dataloader.DataLoaderInterface;
import org.apache.commons.math3.util.Pair;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SqliteDataLoader implements DataLoaderInterface {
    @Override
    public List<Pair<Long, Map>> loadTimeSeries(Properties properties) {
        URL sqliteFile = this.getClass().getClassLoader().getResource(properties.getProperty("fileName"));
        String JDBCUrl = String.format("jdbc:sqlite:%s", sqliteFile.getPath());

        Connection connection;
        Statement statement;

        List<Pair<Long, Map>> data = new ArrayList<>();

        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(JDBCUrl);

            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select dataLength,samplingRate,wave,measdate from main.vibdata");
            resultSet.next();
            int dataLength = resultSet.getInt("dataLength");
            int samplingRate = resultSet.getInt("samplingRate");
            String measDate = resultSet.getString("measDate");
            byte[] wave = resultSet.getBytes("wave");

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long startTimestamp = format.parse(measDate).getTime() * (long) Math.pow(10, 6);
            long interval = (long) (Math.pow(10, 9) / samplingRate);

            for (int i = 0; i < dataLength; i++) {
                byte[] bytes = Arrays.copyOfRange(wave, i * 4, (i+1) * 4);
                float f = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
                long timestamp = startTimestamp + i * interval;
                HashMap<String, Object> row = new HashMap<>();

                row.put("`016`", f);
                row.put("`017`", "1");
                row.put("`018`", "2");
                row.put("`019`", "3");
                row.put("`020`", "4");
                data.add(new Pair(timestamp, row));
            }

            resultSet.close();
            statement.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException | ParseException e) {
            e.printStackTrace();
        }
        return data;
    }
}
