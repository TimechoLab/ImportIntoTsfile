package com.timecho.test;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class SqliteSchema {
    private SqliteSchema() {}
    public static final List<MeasurementSchema> cmsSchema = new ArrayList<>();

    static {
        cmsSchema.add(new MeasurementSchema("`016`",TSDataType.FLOAT));
        cmsSchema.add(new MeasurementSchema("`017`", TSDataType.TEXT));
        cmsSchema.add(new MeasurementSchema("`018`", TSDataType.TEXT));
        cmsSchema.add(new MeasurementSchema("`019`", TSDataType.TEXT));
    }
}
