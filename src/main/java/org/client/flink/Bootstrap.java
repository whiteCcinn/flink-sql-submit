package org.client.flink;

import deps.util.ParameterToolEnvironmentUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.client.flink.cmds.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

abstract public class Bootstrap {

    public static HashMap<String, AbstractCommand> allCmd = new HashMap<>();
    public static ArrayList<String> cmd = new ArrayList<>();

    protected static void registerCommands(String[] args) throws IOException {
        allCmd.put(HelpCommand.name, new HelpCommand());
        allCmd.put(JobCommand.name, new JobCommand());
        allCmd.put(SqlParserCommand.name, new SqlParserCommand());
        allCmd.put(HiveCatalogCommand.name, new HiveCatalogCommand());
        extractCmd(args);
    }

    protected static AbstractCommand getCommand() {
        return allCmd.get(cmd.get(0));
    }

    protected static void extractCmd(String[] args) throws IOException {
        ArrayList<String> newArgsList = new ArrayList<>();
        boolean isOption = false;
        for (String arg : args) {
            newArgsList.add(arg);
            if (arg.startsWith("-")) {
                if (!isOption) {
                    isOption = true;
                }
            } else {
                if (isOption) {
                    isOption = false;
                } else {
                    cmd.add(arg);
                    newArgsList.remove(newArgsList.size() - 1);
                }
            }
        }

        String[] newArgs = new String[newArgsList.size()];

        ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(newArgsList.toArray(newArgs));
        printConfig(parameterTool);
        allCmd.forEach((name, cmd) -> {
            cmd.setParameterTool(parameterTool);
        });
    }

    private static final Logger logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    private static StreamExecutionEnvironment streamExecutionEnvironment;
    private static ExecutionEnvironment executionEnvironment;

    public static StreamExecutionEnvironment getStreamExecutionEnvironment(String[] args) throws IOException {
        if (streamExecutionEnvironment == null) {
            ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
            printConfig(parameterTool);

            Configuration config = getConfiguration(parameterTool);

            if (parameterTool.getBoolean("app.force.remote", false)) {
                streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
            } else {
                streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(config);
            }
            streamExecutionEnvironment.getConfig().setGlobalJobParameters(parameterTool);
        }

        return streamExecutionEnvironment;
    }

    public static ExecutionEnvironment getExecutionEnvironment(String[] args) throws IOException {
        if (executionEnvironment == null) {
            ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
            printConfig(parameterTool);

            Configuration config = getConfiguration(parameterTool);

            if (parameterTool.getBoolean("app.force.remote", false)) {
                executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
            } else {
                executionEnvironment = ExecutionEnvironment.createLocalEnvironment(config);
            }
            executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);
        }
        return executionEnvironment;
    }

    private static Configuration getConfiguration(ParameterTool parameterTool) {
        Configuration config = new Configuration();
        // 自定义jobmanager-webui的端口
        if (parameterTool.has("jm.webui.port")) {
            config.setInteger(RestOptions.PORT, parameterTool.getInt("jm.webui.port"));
        }
        return config;
    }

    private static void printConfig(ParameterTool parameterTool) {
        boolean isDebug = parameterTool.getBoolean("app.config.debug", false);
        boolean isAllDebug = parameterTool.getBoolean("app.config.debug.all", false);
        if (isDebug) {
            Set<String> configKeys = parameterTool.getProperties().stringPropertyNames();
            for (String key : configKeys) {
                logger.info(key + "=" + parameterTool.get(key));

                if (isAllDebug || (key.startsWith("app"))) {
                    System.out.println(key + "=" + parameterTool.get(key));
                }
            }
        }
    }

}
