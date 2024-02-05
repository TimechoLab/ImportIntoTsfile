package com.timecho;

import com.beust.jcommander.JCommander;
import com.timecho.dataloader.DataLoaderInterface;
import com.timecho.datawriter.DataWriter;
import com.timecho.tsfileloader.TsFileLoader;
import org.apache.commons.math3.util.Pair;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.*;

public class LoadData {
    public static void main(String[] args) throws IOException, WriteProcessException, IoTDBConnectionException, StatementExecutionException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchFieldException {
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
        DataLoaderInterface dataLoader = (DataLoaderInterface) dataLoaderClass.newInstance();
        List<Pair<Long, Map<String, Object>>> data = dataLoader.loadTimeSeries(options);

        String schemaOption = options.getSchema();
        String[] split = schemaOption.split("\\.");
        String schemaClassName = String.join(".", Arrays.copyOfRange(split, 0 , split.length - 1));
        String schemaFieldName = split[split.length - 1];

        Class<?> schemaClass = Class.forName(schemaClassName);
        List<MeasurementSchema> schema = (List<MeasurementSchema>) schemaClass
                .getField(schemaFieldName)
                .get(null);

        DataWriter dataWriter = new DataWriter(options, schema, false);
        dataWriter.writeData(data);
        dataWriter.close();

        TsFileLoader.loadTsFile(options);
    }
}