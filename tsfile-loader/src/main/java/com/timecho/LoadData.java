package com.timecho;

import com.beust.jcommander.JCommander;
import com.timecho.dataloader.DataLoaderInterface;
import com.timecho.datawriter.DataWriter;
import com.timecho.model.SchemaAndData;
import com.timecho.tsfileloader.TsFileLoader;
import org.apache.commons.math3.util.Pair;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

public class LoadData {
    public static void main(String[] args) throws Exception {
        LoadCommandOptions options = new LoadCommandOptions();
        JCommander commander = JCommander.newBuilder()
                .addObject(options)
                .build();
        commander.parse(args);

        if (options.isHelp()) {
            commander.usage();
            return;
        }

        Class<?> dataLoaderClass = Class.forName(options.getLoader());
        Method loadTimeSeries = dataLoaderClass.getMethod("loadTimeSeries", LoadCommandOptions.class);
        DataLoaderInterface dataLoader = (DataLoaderInterface) dataLoaderClass.newInstance();
        SchemaAndData schemaAndData = dataLoader.loadTimeSeries(options);

        DataWriter dataWriter = new DataWriter(options, schemaAndData.getSchema(), false);
        dataWriter.writeData(schemaAndData.getData());
        dataWriter.close();

        TsFileLoader.loadTsFile(options);
    }
}