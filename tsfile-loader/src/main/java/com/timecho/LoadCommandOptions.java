package com.timecho;

import com.beust.jcommander.Parameter;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.PathUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadCommandOptions {
    @Parameter(names = {"help"}, description = "查看帮助信息", help = true, order = 1)
    private boolean help;

    @Parameter(names = {"loader", "-l"}, description = "数据加载器类的全限定名", order = 2)
    private String loader;

    @Parameter(names = {"srcPath", "-s"}, description = "源文件路径", required = true, order = 4)
    private String srcPath;

    @Parameter(names = {"targetPath", "-t"}, description = "tsFile存放路径", order = 5)
    private String targetPath;

    @Parameter(names = {"deviceId", "-d"}, description = "deviceId", required = true, order = 6)
    private String deviceId;

    @Parameter(names = {"import", "-i"}, description = "是否导入到 IoTDB", required = false, order = 7)
    private boolean isImport;

    @Parameter(names = {"host", "-h"}, description = "IoTDB Host", order = 8)
    private String host = "127.0.0.1";

    @Parameter(names = {"port", "-p"}, description = "IoTDB Port", order = 9)
    private int port = 6667;

    @Parameter(names = {"username", "-u"}, description = "IoTDB Username", order = 10)
    private String username = "root";

    @Parameter(names = {"password", "-pw"}, description = "IoTDB Password", order = 11)
    private String password = "root";

    @Parameter
    private List<String> mainParameters;

    private Map<String, String> selfDefineOptions;

    public boolean isHelp() {
        return help;
    }

    public String getLoader() {
        return loader;
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

    public String getDeviceId() throws IllegalPathException {
        PathUtils.isLegalPath(deviceId);
        return deviceId;
    }

    public boolean isImport() {
        return isImport;
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
        if (selfDefineOptions == null) {
            selfDefineOptions = new HashMap<>();

            assert mainParameters.size() % 2 == 0;

            for (int i = 0; i < mainParameters.size() / 2; i++) {
                selfDefineOptions.put(mainParameters.get(i), mainParameters.get(i + 1));
            }
        }

        return selfDefineOptions;
    }
}
