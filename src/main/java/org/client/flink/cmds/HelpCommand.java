package org.client.flink.cmds;

import java.util.ArrayList;

public class HelpCommand extends AbstractCommand {
    public static final String name = "help";

    public static String[] HelpText = new String[]{
            "帮助命令",
            "",
            "Usage of \"flink run <.jar> help [options]\"",
            "",
            "Available Commands",
            "   job          提交job作业",
            "   sql-parser   解析sql文件",
            "   help         帮助命令",
            "   hive-catalog hive-catalog的相关",
    };

    public HelpCommand() {
        this.init(HelpText);
    }

    @Override
    public void run(ArrayList<String> cmd) throws Exception {
        this.help();
    }
}
