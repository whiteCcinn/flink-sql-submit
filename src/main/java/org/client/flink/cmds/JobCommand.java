package org.client.flink.cmds;

import deps.util.SqlCommandParser;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.BatchTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.client.flink.enums.PlanType;
import org.client.flink.udfs.UDFIP2LocationFunction;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobCommand extends AbstractCommand {
    public static final String name = "job";

    public Pattern pattern;

    public static String[] HelpText = new String[]{
            "提交job",
            "",
            "Usage of \"flink run <.jar> job [options]\"",
            "   --sql string",
            "       执行的sql (*)",
            "   --plan string",
            "       选择执行计划器:",
            "           flink-streaming",
            "           flink-batch",
            "           blink-streaming",
            "           flink-batch",
    };

    public JobCommand() {
        this.init(HelpText);
        this.pattern = Pattern.compile("\\{(.+?)}", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    }

    @Override
    public void run(ArrayList<String> cmd) throws Exception {
        if (cmd.size() > 1) {
            this.help();
            return;
        }

        List<SqlCommandParser.SqlCommandCall> cmdCalls = null;

        if (this.getParameterTool().has("sql")) {
            String sql = this.getParameterTool().get("sql").trim();
            if (sql.length() == 0) {
                throw new Exception("--sql 请输入sql语句");
            }
            List<String> allSql = Arrays.asList(sql.split(";"));
            for (int i = 0; i < allSql.size(); i++) {
                allSql.set(i, allSql.get(i) + ";");
            }
            cmdCalls = SqlCommandParser.parse(allSql);
        } else if (this.getParameterTool().has("sql-file")) {
            String path = this.getParameterTool().get("sql-file").trim();
            if (path.length() == 0) {
                throw new Exception("--sql-file 请输入sql文件路径");
            }
            List<String> sql = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
            cmdCalls = SqlCommandParser.parse(sql);
        } else {
            throw new Exception("--sql 或者 --sql-file 二选一必填");
        }

        switch (this.getParameterTool().get("plan", PlanType.BLINK_STREAMING)) {
            case PlanType.FLINK_STREAMING:
                EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
                StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
                extracted(cmdCalls, fsTableEnv);
                break;
            case PlanType.FLINK_BATCH:
                ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
                BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);
                extracted(cmdCalls, fbTableEnv);
                break;
            case PlanType.BLINK_STREAMING:
                StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
                StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
                extracted(cmdCalls, bsTableEnv);
                break;
            case PlanType.BLINK_BATCH:
                EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
                TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
                extracted(cmdCalls, bbTableEnv);
                break;
            default:
                throw new Exception("请确保输入的--plan是否正确");
        }

    }

    private void extracted(List<SqlCommandParser.SqlCommandCall> cmdCalls, TableEnvironment fsTableEnv) throws Exception {

        for (SqlCommandParser.SqlCommandCall call : cmdCalls) {
            this.callCommand(call, fsTableEnv);
        }

        Optional<Catalog> catalog = fsTableEnv.getCatalog(fsTableEnv.getCurrentCatalog());
        catalog.ifPresent(Catalog::close);
    }

    private void registerUDFS(TableEnvironment fsTableEnv) {
        HashMap<String, Class<? extends UserDefinedFunction>> udfMap = new HashMap<>();
        udfMap.put(UDFIP2LocationFunction.functionName, UDFIP2LocationFunction.class);

        /*
        // tips: 目前发现sql模式只能运行TemporarySystemFunction，所以这里无法持久化函数
        // 并且注册未user-function的话，需要把jar放在flink的lib目录下，否则会报class not found
        List<String> listFunc = Arrays.asList(fsTableEnv.listUserDefinedFunctions());
        udfMap.forEach((k, v) -> {
            if (!listFunc.contains(k)) {
                fsTableEnv.createFunction(k, v, true);
            }
        });
        */

        // 注册在临时系统udf中
        List<String> listFunc = Arrays.asList(fsTableEnv.listFunctions());
        udfMap.forEach((k, v) -> {
            if (!listFunc.contains(k)) {
                fsTableEnv.createTemporarySystemFunction(k, v);
            }
        });
    }

    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment env) throws Exception {
        switch (cmdCall.command) {
            case CATALOG_INFO:
                callCatalog(cmdCall, env);
                break;
            case CREATE_DATABASE:
                callCreateDatabase(cmdCall, env);
                break;
            case USE:
                callUse(cmdCall, env);
                break;
            case SET:
                callSet(cmdCall, env);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall, env);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall, env);
                break;
            case SELECT:
                callSelect(cmdCall, env);
                break;
            case CREATE_VIEW:
                callCreateView(cmdCall, env);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callCatalog(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) {
        String catalogType = cmdCall.operands[0].trim();
        String hiveConf = cmdCall.operands[1].trim();
        String catalogName = cmdCall.operands[2].trim();

        Catalog catalog = null;
        if (catalogType.equals(SqlCommandParser.DefaultCatalogType)) {
            catalog = new HiveCatalog(catalogName, null, hiveConf);
        } else {
            catalog = new GenericInMemoryCatalog(catalogName);
        }

        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);
        this.registerUDFS(tEnv);
    }

    private void callCreateDatabase(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) throws Exception {
        String dbName = cmdCall.operands[0].trim();
        String comment = cmdCall.operands[1].trim();
        Optional<Catalog> catalog = tEnv.getCatalog(tEnv.getCurrentCatalog());
        if (catalog.isPresent()) {
            catalog.get().createDatabase(dbName, new CatalogDatabaseImpl(new HashMap<>(), comment), true);
        } else {
            throw new Exception("请先设置catalog信息");
        }
    }

    private void callUse(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) {
        String dbName = cmdCall.operands[0].trim();
        tEnv.useDatabase(dbName);
    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) throws Exception {
        String key = cmdCall.operands[0].trim().replaceAll("'", "").replaceAll("\"", "");
        String value = cmdCall.operands[1].trim();

        Matcher matcher = this.pattern.matcher(value);
        int matcher_start = 0;
        while (matcher.find(matcher_start)) {
            for (int i = 0; i < matcher.groupCount(); i++) {
                String var = matcher.group(i + 1);
                String kv;
                if (var.equals("db")) {
                    kv = tEnv.getCurrentDatabase();
                } else {
                    kv = tEnv.getConfig().getConfiguration().toMap().get(var);
                }
                value = value.replaceAll("\\{" + var + "}", kv);
            }
            matcher_start = matcher.end();
        }

        if (key.startsWith("mc.")) {
            if (key.equals(SqlCommandParser.MCSet.MC_IDLE_STATE_RETENTION_TIME.toString())) {
                tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(Long.parseLong(value)));
                return;
            } else if (key.equals(SqlCommandParser.MCSet.MC_LOCAL_TIME_ZONE.toString())) {
                tEnv.getConfig().setLocalTimeZone(ZoneId.of(value));
                return;
            } else if (key.equals(SqlCommandParser.MCSet.MC_PARALLELISM_DEFAULT.toString())) {
                if (tEnv instanceof StreamTableEnvironmentImpl) {
                    ((StreamTableEnvironmentImpl) tEnv).execEnv().setParallelism(Integer.parseInt(value));
                } else if (tEnv instanceof BatchTableEnvironmentImpl) {
                    ((BatchTableEnvironmentImpl) tEnv).execEnv().setParallelism(Integer.parseInt(value));
                } else {
                    throw new Exception("该模式不支持设置调用 setParallelism()");
                }
            } else if (key.equals(SqlCommandParser.MCSet.MC_STATE_BACKEND_FS_CHECKPOINTDIR.toString())) {
                if (tEnv instanceof StreamTableEnvironmentImpl) {
                    ((StreamTableEnvironmentImpl) tEnv).execEnv().setStateBackend(new FsStateBackend(value, true));
                } else if (tEnv instanceof BatchTableEnvironmentImpl) {
                    throw new Exception("该模式不支持设置调用 setStateBackend()");
                }
            }

            tEnv.getConfig().getConfiguration().setString(key, value);
            return;
        }

        if (value.toLowerCase().trim().equals("true") || value.toLowerCase().trim().equals("false")) {
            tEnv.getConfig().getConfiguration().setBoolean(key, Boolean.parseBoolean(value));
        } else {
            try {
                tEnv.getConfig().getConfiguration().setInteger(key, Integer.parseInt(value));
            } catch (NumberFormatException e) {
                tEnv.getConfig().getConfiguration().setString(key, value);
            }
        }
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) throws TableNotExistException {
        String ddl = cmdCall.operands[0].trim();
        String table = cmdCall.operands[1].trim();
        Optional<Catalog> catalog = tEnv.getCatalog(tEnv.getCurrentCatalog());
        if (!catalog.isPresent()) {
            throw new RuntimeException("请先设置catalog信息");
        }
        ObjectPath op = new ObjectPath(tEnv.getCurrentDatabase(), table);
        if (catalog.get().tableExists(op)) {
            return;
        }

        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callCreateView(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) {
        String ddl = cmdCall.operands[0].trim();
        Optional<Catalog> catalog = tEnv.getCatalog(tEnv.getCurrentCatalog());
        if (!catalog.isPresent()) {
            throw new RuntimeException("请先设置catalog信息");
        }

        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) {
        String dml = cmdCall.operands[0].trim();
        try {
            tEnv.executeSql(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
        System.out.println(tEnv.getConfig().getConfiguration().toMap());
    }

    private void callSelect(SqlCommandParser.SqlCommandCall cmdCall, TableEnvironment tEnv) {
        String dml = cmdCall.operands[0].trim();
        try {
            TableResult result = tEnv.executeSql(dml);
            result.print();
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}
