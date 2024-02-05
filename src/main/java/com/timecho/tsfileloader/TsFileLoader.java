package com.timecho.tsfileloader;

import com.timecho.LoadCommandOptions;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.io.File;

public class TsFileLoader {
    private TsFileLoader () {}

    public static void loadTsFile(LoadCommandOptions options) throws IoTDBConnectionException, StatementExecutionException {
        final Session session = new Session.Builder()
                .host(options.getHost())
                .port(options.getPort())
                .username(options.getUsername())
                .password(options.getPassword())
                .build();
        session.open();

        String loadSQL = String.format("load '%s' onSuccess=none", new File(options.getTargetPath()).getAbsolutePath());
        session.executeNonQueryStatement(loadSQL);

        session.close();
    }
}
