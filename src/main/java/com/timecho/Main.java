package com.timecho;

import com.timecho.dataloader.impl.SqliteDataLoader;
import com.timecho.datawriter.DataWriter;
import org.apache.commons.math3.util.Pair;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.*;

public class Main {
    static SqliteDataLoader dataLoader;
    public static void main(String[] args) throws IOException, WriteProcessException {
        dataLoader = new SqliteDataLoader();
        Properties properties = new Properties();
        properties.put("fileName", "1002_CMS_016_20211216230132_X.sqlite");
        List<Pair<Long, Map<String, Object>>> data = dataLoader.loadTimeSeries(properties);

        ArrayList<MeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("`016`", TSDataType.FLOAT));
        schemas.add(new MeasurementSchema("`017`", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("`018`", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("`019`", TSDataType.TEXT));
        DataWriter dataWriter = new DataWriter("1002_CMS_016_20211216230132_X.tsfile", "root.db.`1002`.CMS", schemas, false);

        dataWriter.writeData(data);
        dataWriter.close();
    }
}