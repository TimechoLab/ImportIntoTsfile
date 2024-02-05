package com.timecho;

import com.beust.jcommander.Parameter;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadCommandOptions {
    @Parameter(names = {"help"}, description = "查看帮助信息", help = true)
    private boolean help;

    @Parameter(names = {"loader", "-l"}, description = "数据加载器类的全限定名", required = true)
    private String loader;

    @Parameter(names = {"schemaPath", "-sp"}, description = "元数据属性路径", required = true)
    private String schema;

    @Parameter(names = {"srcPath", "-s"}, description = "源文件路径", required = true)
    private String srcPath;

    @Parameter(names = {"targetPath", "-t"}, description = "tsFile存放路径")
    private String targetPath;

    @Parameter(names = {"deviceId", "-d"}, description = "deviceId", required = true)
    private String deviceId;

    @Parameter(names = {"host", "-h"}, description = "IoTDB Host")
    private String host = "127.0.0.1";

    @Parameter(names = {"port", "-p"}, description = "IoTDB Port")
    private int port = 6667;

    @Parameter(names = {"username", "-u"}, description = "IoTDB Username")
    private String username = "root";

    @Parameter(names = {"password", "-pw"}, description = "IoTDB Password")
    private String password = "root";

    @Parameter
    private List<String> mainParameters;

    public boolean isHelp() {
        return help;
    }

    public String getLoader() {
        return loader;
    }

    public String getSchema() {
        return schema;
    }

    public String getSrcPath() {
        return srcPath;
    }

    public String getTargetPath() {
        if (targetPath == null) {
            File srcFile = new File(srcPath);
            return srcFile.getName() + ".tsfile";
        }
        assert targetPath.endsWith(".tsfile");
        return targetPath;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public List<String> getMainParameters() {
        return mainParameters;
    }

    public Map<String, String> getSelfDefineOptions() {
        Map<String, String> selfDefineOptions = new HashMap<>();
        assert mainParameters.size() % 2 == 0;

        for (int i = 0; i < mainParameters.size() / 2; i++) {
            selfDefineOptions.put(mainParameters.get(i), mainParameters.get(i + 1));
        }

        return selfDefineOptions;
    }
}
