package com.timecho.dataloader;

import com.timecho.LoadCommandOptions;
import com.timecho.model.SchemaAndData;
import org.apache.commons.math3.util.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;
import java.util.Map;

public interface DataLoaderInterface {
    SchemaAndData loadTimeSeries(LoadCommandOptions options);
}
