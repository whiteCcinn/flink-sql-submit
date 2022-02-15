package org.client.flink.cmds;

import deps.util.SqlCommandParser;
import lombok.Builder;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.client.flink.enums.PlanType;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Builder
public class SqlParserCommand extends AbstractCommand {
    public static final String name = "sql-parser";

    public static String[] HelpText = new String[]{
            "SQL解析器",
            "",
            "Usage of \"flink run <.jar> job [options]\"",
            "   --sql-file string",
            "       包含sql的文件 (*)",
    };

    public SqlParserCommand() {
        this.init(HelpText);
    }

    @Override
    public void run(ArrayList<String> cmd) throws Exception {
        if (cmd.size() > 1) {
            this.help();
            return;
        }

        // 校验参数 --sql
        if (!this.getParameterTool().has("sql-file")) {
            throw new Exception("--sql-file参数必填");
        } else if (this.getParameterTool().get("sql-file").trim().length() == 0) {
            throw new Exception("--sql-file 不允许为空");
        }

        String file = this.getParameterTool().get("sql-file").trim();

        List<String> sql = Files.readAllLines(Paths.get(file));

        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            System.out.println(call);
        }
    }
}
