package org.client.flink.cmds;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.Arrays;

@Setter
@Getter
abstract public class AbstractCommand implements ICommand {
    ParameterTool parameterTool;
    private static String[] helpText = new String[]{
            "",
            "Global Options:",
            "   --app.force.remote bool",
            "       是否启动远端环境变量: false",
            "   --app.config.debug bool",
            "       是否打印用户参数: false"
    };

    private String text;

    public void init(String[] helpText) {
        ArrayList<String> textList = new ArrayList<>(Arrays.asList(helpText));
        textList.addAll(Arrays.asList(AbstractCommand.helpText));
        text = String.join("\n", textList);
    }

    protected void help() {
        System.out.println(this.text);
    }
}
