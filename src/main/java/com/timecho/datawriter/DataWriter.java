package com.timecho.datawriter;

import org.apache.commons.math3.util.Pair;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.*;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataWriter {
    private TsFileWriter tsFileWriter;
    private List<MeasurementSchema> schemas;
    private Boolean aligned;
    private String deviceId;

    public DataWriter(String tsfileName, String deviceId, ArrayList<MeasurementSchema> schemas, Boolean aligned) {
        this.aligned = aligned;
        this.deviceId = deviceId;
        this.schemas = schemas;

        File tsfile = FSFactoryProducer.getFSFactory().getFile(tsfileName);
        if (tsfile.exists()) {
            tsfile.delete();
        }

        try {
            tsFileWriter = new TsFileWriter(tsfile);
            if (aligned) {
                tsFileWriter.registerAlignedTimeseries(new Path(deviceId), schemas);
            } else {
                tsFileWriter.registerTimeseries(new Path(deviceId), schemas);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeData(List<Pair<Long, Map>> alignedData) throws IOException, WriteProcessException {
        for (Pair<Long, Map> row : alignedData) {
            Long timeStamp = row.getKey();
            Map values = row.getValue();

            TSRecord tsRecord = new TSRecord(timeStamp, deviceId);
            for (MeasurementSchema measurementSchema : this.schemas) {
                String measurementId = measurementSchema.getMeasurementId();
                TSDataType type = measurementSchema.getType();
                Object value = values.get(measurementId);
                if (value != null) {
                    tsRecord.addTuple(getDataPoint(type, measurementId, value));
                }
            }
            if (aligned) {
                tsFileWriter.writeAligned(tsRecord);
            } else {
                tsFileWriter.write(tsRecord);
            }
        }
    }

    public void flush() throws IOException {
        tsFileWriter.flushAllChunkGroups();
    }

    public void close() throws IOException {
        this.flush();
        tsFileWriter.close();
    }

    private DataPoint getDataPoint(TSDataType dataType, String measurementId, Object value) {
        switch (dataType) {
            case TEXT: return new StringDataPoint(measurementId, Binary.valueOf((String) value));
            case BOOLEAN: return new BooleanDataPoint(measurementId, (boolean) value);
            case INT32: return new IntDataPoint(measurementId, (int) value);
            case INT64: return new LongDataPoint(measurementId, (long) value);
            case FLOAT: return new FloatDataPoint(measurementId, (float) value);
            default: return new DoubleDataPoint(measurementId, (double) value);
        }
    }
}
