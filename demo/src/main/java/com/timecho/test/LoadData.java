package com.timecho.test;

import com.beust.jcommander.JCommander;
import com.timecho.LoadCommandOptions;
import com.timecho.datawriter.DataWriter;
import com.timecho.model.SchemaAndData;
import com.timecho.tsfileloader.TsFileLoader;

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

        SqliteDataLoader dataLoader = new SqliteDataLoader();
        SchemaAndData schemaAndData = dataLoader.loadTimeSeries(options);

        DataWriter dataWriter = new DataWriter(options, schemaAndData.getSchema(), false);
        dataWriter.writeData(schemaAndData.getData());
        dataWriter.close();

        TsFileLoader.loadTsFile(options);
    }
}
