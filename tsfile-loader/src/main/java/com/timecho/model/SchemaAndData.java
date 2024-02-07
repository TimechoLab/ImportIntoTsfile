package com.timecho.model;

import org.apache.commons.math3.util.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;
import java.util.Map;

public class SchemaAndData {
    private final List<MeasurementSchema> schema;
    private final List<Pair<Long, Map<String, Object>>> data;

    public SchemaAndData(List<MeasurementSchema> schema, List<Pair<Long, Map<String, Object>>> data) {
        this.schema = schema;
        this.data = data;
    }

    public List<MeasurementSchema> getSchema() {
        return schema;
    }

    public List<Pair<Long, Map<String, Object>>> getData() {
        return data;
    }
}
