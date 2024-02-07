package com.timecho.test;

import com.timecho.LoadCommandOptions;
import com.timecho.dataloader.DataLoaderInterface;
import com.timecho.model.SchemaAndData;
import org.apache.commons.math3.util.Pair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SqliteDataLoader implements DataLoaderInterface {
    public static final List<MeasurementSchema> CMS_SCHEMA = new ArrayList<>();

    static {
        CMS_SCHEMA.add(new MeasurementSchema("`016`", TSDataType.FLOAT));
        CMS_SCHEMA.add(new MeasurementSchema("`017`", TSDataType.TEXT));
        CMS_SCHEMA.add(new MeasurementSchema("`018`", TSDataType.TEXT));
        CMS_SCHEMA.add(new MeasurementSchema("`019`", TSDataType.TEXT));
    }

    public SchemaAndData loadTimeSeries(LoadCommandOptions options) {
        File sqliteFile = new File(options.getSrcPath());
        String jdbcUrl = String.format("jdbc:sqlite:%s", sqliteFile.getPath());

        Connection connection;
        Statement statement;

        List<Pair<Long, Map<String, Object>>> data = new ArrayList<>();

        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(jdbcUrl);

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
                data.add(new Pair<>(timestamp, row));
            }

            resultSet.close();
            statement.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException | ParseException e) {
            e.printStackTrace();
        }
        return new SchemaAndData(CMS_SCHEMA, data);
    }
}