package org.client.flink.cmds;

import deps.util.SqlCommandParser;
import lombok.Builder;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Builder
public class HiveCatalogCommand extends AbstractCommand {
    public static final String name = "hive-catalog";

    public static String[] HelpText = new String[]{
            "hive-catalog",
            "",
            "Usage of \"flink run <.jar> hive-catalog <child-command>\"",
            "   --hive-conf string",
            "       hive的配置文件",
            "",
            "Child-command:",
            "   create_database 创建数据库",
            "   list_database   列出数据库",
            "   drop_database   删除数据库",
            "   list_table      列出数据库的所有表",
            "   drop_table      删除表",
            "   list_view       列出所有数据库的视图"
    };

    public HiveCatalogCommand() {
        this.init(HelpText);
    }

    @Override
    public void run(ArrayList<String> cmd) throws Exception {
        if (cmd.size() > 3) {
            this.help();
            return;
        }

        switch (cmd.get(1)) {
            case CreateDatabaseCommand.name:
                CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand();
                createDatabaseCommand.setParameterTool(this.getParameterTool());
                createDatabaseCommand.run(new ArrayList<>(cmd.subList(1, cmd.size())));
                break;
            case ListDatabaseCommand.name:
                ListDatabaseCommand listDatabaseCommand = new ListDatabaseCommand();
                listDatabaseCommand.setParameterTool(this.getParameterTool());
                listDatabaseCommand.run(new ArrayList<>(cmd.subList(1, cmd.size())));
                break;
            case DropDatabaseCommand.name:
                DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand();
                dropDatabaseCommand.setParameterTool(this.getParameterTool());
                dropDatabaseCommand.run(new ArrayList<>(cmd.subList(1, cmd.size())));
                break;
            case ListTableCommand.name:
                ListTableCommand listTableCommand = new ListTableCommand();
                listTableCommand.setParameterTool(this.getParameterTool());
                listTableCommand.run(new ArrayList<>(cmd.subList(1, cmd.size())));
                break;
            case DropTableCommand.name:
                DropTableCommand dropTableCommand = new DropTableCommand();
                dropTableCommand.setParameterTool(this.getParameterTool());
                dropTableCommand.run(new ArrayList<>(cmd.subList(1, cmd.size())));
                break;
            case ListViewCommand.name:
                ListViewCommand listViewCommand = new ListViewCommand();
                listViewCommand.setParameterTool(this.getParameterTool());
                listViewCommand.run(new ArrayList<>(cmd.subList(1, cmd.size())));
            default:
                this.help();
        }
    }

    public static class CreateDatabaseCommand extends AbstractCommand {

        public static final String name = "create_database";

        public String[] HelpText = new String[]{
                "hive-catalog create_database",
                "",
                "Usage of \"flink run <.jar> hive-catalog create_database\"",
                "   --hive-conf string",
                "       hive的配置文件",
                "   --db_name string",
                "       数据库名字 (*)",
                "   --db_comment string",
                "       数据库注释",
                "   --catalog_name string",
                "       catalog名字",
        };

        public CreateDatabaseCommand() {
            this.init(this.HelpText);
        }

        @Override
        public void run(ArrayList<String> cmd) throws Exception {
            if (cmd.size() != 1) {
                this.help();
                return;
            }

            if (!this.getParameterTool().has("db_name")) {
                throw new Exception("--db_name 必填");
            }
            String dbName = this.getParameterTool().get("db_name").trim();

            String hiveConf = "";
            if (this.getParameterTool().has("hive-conf")) {
                hiveConf = this.getParameterTool().get("hive-conf").trim();
            } else {
                hiveConf = SqlCommandParser.DefaultHiveConf;
            }

            String comment = "";
            if (this.getParameterTool().has("comment")) {
                comment = this.getParameterTool().get("comment").trim();
            }

            String catalogName = SqlCommandParser.DefaultCatalogName;
            if (this.getParameterTool().has("catalog_name")) {
                catalogName = this.getParameterTool().get("catalog_name").trim();
            }

            Catalog catalog = new HiveCatalog(catalogName, null, hiveConf);
            catalog.open();
            catalog.createDatabase(dbName, new CatalogDatabaseImpl(new HashMap<>(), comment), false);
            System.out.println(catalog.listDatabases());
            catalog.close();
        }
    }

    public static class ListDatabaseCommand extends AbstractCommand {

        public static final String name = "list_database";

        public String[] HelpText = new String[]{
                "hive-catalog list_database",
                "",
                "Usage of \"flink run <.jar> hive-catalog list_database\"",
                "   --hive-conf string",
                "       hive的配置文件",
                "   --catalog_name string",
                "       catalog名字",
        };

        public ListDatabaseCommand() {
            this.init(this.HelpText);
        }

        @Override
        public void run(ArrayList<String> cmd) throws Exception {
            if (cmd.size() != 1) {
                this.help();
                return;
            }
            String hiveConf = "";
            if (this.getParameterTool().has("hive-conf")) {
                hiveConf = this.getParameterTool().get("hive-conf").trim();
            } else {
                hiveConf = SqlCommandParser.DefaultHiveConf;
            }

            String catalogName = SqlCommandParser.DefaultCatalogName;
            if (this.getParameterTool().has("catalog_name")) {
                catalogName = this.getParameterTool().get("catalog_name").trim();
            }

            Catalog catalog = new HiveCatalog(catalogName, null, hiveConf);
            catalog.open();
            System.out.println(catalog.listDatabases());
            catalog.close();
        }
    }

    public static class DropDatabaseCommand extends AbstractCommand {

        public static final String name = "drop_database";

        public String[] HelpText = new String[]{
                "hive-catalog drop_database",
                "",
                "Usage of \"flink run <.jar> hive-catalog drop_database\"",
                "   --hive-conf string",
                "       hive的配置文件",
                "   --catalog_name string",
                "       catalog名字",
                "   --db_name string",
                "       数据库名字 (*)",
        };

        public DropDatabaseCommand() {
            this.init(HelpText);
        }

        @Override
        public void run(ArrayList<String> cmd) throws Exception {
            if (cmd.size() != 1) {
                this.help();
                return;
            }

            if (!this.getParameterTool().has("db_name")) {
                throw new Exception("--db_name 必填");
            }
            String dbName = this.getParameterTool().get("db_name").trim();

            String hiveConf = "";
            if (this.getParameterTool().has("hive-conf")) {
                hiveConf = this.getParameterTool().get("hive-conf").trim();
            } else {
                hiveConf = SqlCommandParser.DefaultHiveConf;
            }

            String catalogName = SqlCommandParser.DefaultCatalogName;
            if (this.getParameterTool().has("catalog_name")) {
                catalogName = this.getParameterTool().get("catalog_name").trim();
            }

            Catalog catalog = new HiveCatalog(catalogName, null, hiveConf);
            catalog.open();
            catalog.dropDatabase(dbName, true);
            System.out.println(catalog.listDatabases());
            catalog.close();
        }
    }

    public static class ListTableCommand extends AbstractCommand {

        public static final String name = "list_table";

        public String[] HelpText = new String[]{
                "hive-catalog list_table",
                "",
                "Usage of \"flink run <.jar> hive-catalog list_table\"",
                "   --hive-conf string",
                "       hive的配置文件",
                "   --catalog_name string",
                "       catalog名字",
                "   --db_name string",
                "       数据库名字 (*)",
        };

        public ListTableCommand() {
            this.init(this.HelpText);
        }

        @Override
        public void run(ArrayList<String> cmd) throws Exception {
            if (cmd.size() != 1) {
                this.help();
                return;
            }

            if (!this.getParameterTool().has("db_name")) {
                throw new Exception("--db_name 必填");
            }
            String dbName = this.getParameterTool().get("db_name").trim();

            String hiveConf = "";
            if (this.getParameterTool().has("hive-conf")) {
                hiveConf = this.getParameterTool().get("hive-conf").trim();
            } else {
                hiveConf = SqlCommandParser.DefaultHiveConf;
            }

            String catalogName = SqlCommandParser.DefaultCatalogName;
            if (this.getParameterTool().has("catalog_name")) {
                catalogName = this.getParameterTool().get("catalog_name").trim();
            }

            HiveCatalog catalog = new HiveCatalog(catalogName, null, hiveConf);

            catalog.open();
            System.out.println(catalog.listTables(dbName));
            catalog.close();
        }
    }


    public static class DropTableCommand extends AbstractCommand {

        public static final String name = "drop_table";

        public String[] HelpText = new String[]{
                "hive-catalog drop_table",
                "",
                "Usage of \"flink run <.jar> hive-catalog drop_table\"",
                "   --hive-conf string",
                "       hive的配置文件",
                "   --catalog_name string",
                "       catalog名字",
                "   --db_name string",
                "       数据库名字 (*)",
                "   --table_name string",
                "       表名 (*)",
        };

        public DropTableCommand() {
            this.init(this.HelpText);
        }

        @Override
        public void run(ArrayList<String> cmd) throws Exception {
            if (cmd.size() != 1) {
                this.help();
                return;
            }

            if (!this.getParameterTool().has("db_name")) {
                throw new Exception("--db_name 必填");
            }

            if (!this.getParameterTool().has("table_name")) {
                throw new Exception("--table_name 必填");
            }

            String dbName = this.getParameterTool().get("db_name").trim();
            String tableName = this.getParameterTool().get("table_name").trim();

            String hiveConf = "";
            if (this.getParameterTool().has("hive-conf")) {
                hiveConf = this.getParameterTool().get("hive-conf").trim();
            } else {
                hiveConf = SqlCommandParser.DefaultHiveConf;
            }

            String catalogName = SqlCommandParser.DefaultCatalogName;
            if (this.getParameterTool().has("catalog_name")) {
                catalogName = this.getParameterTool().get("catalog_name").trim();
            }

            String[] tables = tableName.split(",");

            HiveCatalog catalog = new HiveCatalog(catalogName, null, hiveConf);

            catalog.open();
            for (String table : tables) {
                catalog.dropTable(new ObjectPath(dbName, table), true);
            }
            System.out.println(catalog.listTables(dbName));
            catalog.close();
        }
    }

    public static class ListViewCommand extends AbstractCommand {

        public static final String name = "list_view";

        public String[] HelpText = new String[]{
                "hive-catalog list_view",
                "",
                "Usage of \"flink run <.jar> hive-catalog list_view\"",
                "   --hive-conf string",
                "       hive的配置文件",
                "   --catalog_name string",
                "       catalog名字",
                "   --db_name string",
                "       数据库名字 (*)",
        };

        public ListViewCommand() {
            this.init(this.HelpText);
        }

        @Override
        public void run(ArrayList<String> cmd) throws Exception {
            if (cmd.size() != 1) {
                this.help();
                return;
            }

            if (!this.getParameterTool().has("db_name")) {
                throw new Exception("--db_name 必填");
            }
            String dbName = this.getParameterTool().get("db_name").trim();

            String hiveConf = "";
            if (this.getParameterTool().has("hive-conf")) {
                hiveConf = this.getParameterTool().get("hive-conf").trim();
            } else {
                hiveConf = SqlCommandParser.DefaultHiveConf;
            }

            String catalogName = SqlCommandParser.DefaultCatalogName;
            if (this.getParameterTool().has("catalog_name")) {
                catalogName = this.getParameterTool().get("catalog_name").trim();
            }

            HiveCatalog catalog = new HiveCatalog(catalogName, null, hiveConf);

            catalog.open();
            System.out.println(catalog.listViews(dbName));
            catalog.close();
        }
    }
}
