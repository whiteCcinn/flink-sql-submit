package deps.util;

import lombok.experimental.UtilityClass;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.curator4.com.google.common.collect.Maps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

@UtilityClass
public class ParameterToolEnvironmentUtils {
    public static final String defaultConfigFileName = "application.properties";
    public static final String environmentFileNameTemplate = "application-%s.properties";
    public static final String ENV_ACTIVE = "env.active";
    ParameterTool fromArgs;
    ParameterTool systemProperties;

    private ParameterTool fromPropertiesFileUtf8(String filename) throws IOException {
        Properties props = new Properties();
        String userDir;
        if (fromArgs.has("user.dir")) {
            userDir = fromArgs.get("user.dir");
            if (!userDir.endsWith(File.separator)) {
                userDir += File.separator;
            }
        } else {
            userDir =
                    System.getProperty("user.dir")
                            + File.separator
                            + "src"
                            + File.separator
                            + "main"
                            + File.separator
                            + "resources"
                            + File.separator;
        }

        if (new File(userDir).exists()) {
            props.load(new InputStreamReader(new FileInputStream(userDir + filename), StandardCharsets.UTF_8));
        } else {
            props.load(new InputStreamReader(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(filename)), StandardCharsets.UTF_8));
        }
        return ParameterTool.fromMap(Maps.fromProperties(props));
    }

    public ParameterTool createParameterTool(String[] args) throws IOException {
        // 系统环境参数 -Dyy=xx
        systemProperties = ParameterTool.fromSystemProperties();
        // 运行参数 main参数  flink自己数据需要 - 或者 -- 开头做key 例如 -name tom or --name tom
        fromArgs = ParameterTool.fromArgs(args);
        // 默认配置文件
        ParameterTool defaultPropertiesFile = fromPropertiesFileUtf8(defaultConfigFileName);
        // 按照优先级获取有效环境值
        String envActiveValue = getEnvActiveValue(systemProperties, fromArgs, defaultPropertiesFile);
        String currentEnvFileName = String.format(environmentFileNameTemplate, envActiveValue);
        // 读取合并环境参数
        ParameterTool currentEnvPropertiesFile = fromPropertiesFileUtf8(currentEnvFileName);

        return currentEnvPropertiesFile.mergeWith(defaultPropertiesFile).mergeWith(fromArgs).mergeWith(systemProperties);
    }

    /**
     * 按照优先级获取有效环境值
     *
     * @return
     */
    public String getEnvActiveValue(ParameterTool systemProperties, ParameterTool fromArgs, ParameterTool defaultPropertiesFile) {
        //选择参数环境
        String env;
        if (systemProperties.has(ENV_ACTIVE)) {
            env = systemProperties.get(ENV_ACTIVE);
        } else if (fromArgs.has(ENV_ACTIVE)) {
            env = fromArgs.get(ENV_ACTIVE);
        } else if (defaultPropertiesFile.has(ENV_ACTIVE)) {
            env = defaultPropertiesFile.get(ENV_ACTIVE);
        } else {
            //如果没有配置抛出异常
            throw new IllegalArgumentException(String.format("%s does not exist！ Please set up the environment. for example： application.properties Add configuration env.active = dev", ENV_ACTIVE));
        }
        return env;
    }
}
