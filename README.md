# flink-sql-submit
🚀flink-sql-submit is a custom SQL submission client  This is a customizable extension of the client, unlike flink's official default client.


# 创建应用

```shell
yarn-session.sh -jm 1024 -tm 1024 -s 16 -nm '告警流计算应用' -yd
```

# 例子

```shell
# help
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar help
帮助命令

Usage of "flink run <.jar> help [options]"

Available Commands
   job          提交job作业
   sql-parser   解析sql文件
   help         帮助命令
   hive-catalog hive-catalog的相关

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false


# job
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar job help
提交job

Usage of "flink run <.jar> job [options]"
   --sql string
       执行的sql (*)
   --plan string
       选择执行计划器:
           flink-streaming
           flink-batch
           blink-streaming
           flink-batch

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false

# sql-parser
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar sql-parser help
SQL解析器

Usage of "flink run <.jar> job [options]"
   --sql-file string
       包含sql的文件 (*)

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false

# sql-parser
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar sql-parser help
SQL解析器

Usage of "flink run <.jar> job [options]"
   --sql-file string
       包含sql的文件 (*)

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false

# hive-catalog
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar hive-catalog help
hive-catalog

Usage of "flink run <.jar> hive-catalog <child-command>"
   --hive-conf string
       hive的配置文件

Child-command:
   create_database 创建数据库
   list_database   列出数据库
   drop_database   删除数据库
   list_table      列出数据库的所有表

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false
       
# hive-catalog create_database
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar hive-catalog create_database help
hive-catalog create_database

Usage of "flink run <.jar> hive-catalog create_database"
   --hive-conf string
       hive的配置文件
   --db_name string
       数据库名字 (*)
   --db_comment string
       数据库注释
   --catalog_name string
       catalog名字

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false
       
# hive-catalog list_database
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar hive-catalog list_database help
hive-catalog list_database

Usage of "flink run <.jar> hive-catalog list_database"
   --hive-conf string
       hive的配置文件
   --catalog_name string
       catalog名字

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false
       
# hive-catalog drop_database
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar hive-catalog drop_database help
hive-catalog drop_database

Usage of "flink run <.jar> hive-catalog drop_database"
   --hive-conf string
       hive的配置文件
   --catalog_name string
       catalog名字
   --db_name string
       数据库名字 (*)

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false
       
# hive-catalog list_table
root@41c5967b5948:/www# flink run target/mc-flink-sql-submit-1.0-SNAPSHOT.jar hive-catalog list_table help
hive-catalog list_table

Usage of "flink run <.jar> hive-catalog list_table"
   --hive-conf string
       hive的配置文件
   --catalog_name string
       catalog名字
   --db_name string
       数据库名字 (*)

Global Options:
   --app.force.remote bool
       是否启动远端环境变量: false
   --app.config.debug bool
       是否打印用户参数: false
```

# flink-stream-sql-mctl 用法

这是一个集成脚本，所以存在约定的规则和部署的架构约束。

这便于我们管理所有的applition和flink种的所有flink-job。

```shell
➜  flink-sql-submit git:(master) ✗ ./flink-stream-sql-mctl.sh

  flink-stream-sql-mctl.sh [OPTION] <COMMAND>

  Flink流计算SQL-Client的执行脚本

  Command:
    run          [FILE]            运行
    stop         [FILE]            停止
    list         [FILE]            列出FILE所在yid下的所有job任务列表
    drop_table   [FILE]            删除所有表
    rebuild_run  [FILE]            删除所有表，然后重跑(继承savepoint）

  Command-Common-Options:
    -c, --clientpath  [LEVEL]    flink-sql-submit.jar路径  (Default is '/data/tmp/mc-flink-sql-submit-1.0-SNAPSHOT.jar')
    -f   是否强制运行，忽略以往savepoint

  Common-Options:
    -h, --help              Display this help and exit
    --loglevel [LEVEL]      One of: FATAL, ERROR, WARN, INFO, NOTICE, DEBUG, ALL, OFF
                            (Default is 'ERROR')
    --logfile [FILE]        Full PATH to logfile.  (Default is '/Users/caiwenhui/logs/flink-stream-sql-mctl.sh.log')
    -n, --dryrun            Non-destructive. Makes no permanent changes.
    -q, --quiet             Quiet (no output)
    -v, --verbose           Output more information. (Items echoed to 'verbose')
    --force                 Skip all user interaction.  Implied 'Yes' to all actions.

```

约定规则：

- 模型所在父目录的至少有一个yid文件（取最近的一个父节点的yid）对应所在的应用id
- 默认情况下，模型启动的时候会取最近一次`savepoint`的数据进行恢复，如果不存在，则直接启动

## 停止所有模型

```shell
for i in $(find /data/flink-stream/mstream_alarm/ -type f -name "*.sql");do /data/flink-stream/flink-stream-sql-mctl stop $i;done
```

## 启动所有模型

```shell
for i in $(find /data/flink-stream/mstream_alarm/ -type f -name "*.sql");do /data/flink-stream/flink-stream-sql-mctl run $i;done
```

## 删除所有表

```shell
for i in $(find /data/flink-stream/mstream_alarm/ -type f -name "*.sql");do /data/flink-stream/flink-stream-sql-mctl drop_table $i;done
```

# 服务器部署结构

```
/data/flink-stream/
├── flink-stream-sql-mctl
├── inc.sh
├── mc-flink-sql-submit-1.0-SNAPSHOT.jar
├── mstream
│   └── mstream_19
│       ├── c_log_new_add_account_by_upf_account.sql
│       ├── ...
│       └── yid
└── mstream_alarm
    ├── mstream_alarm_10008
    │   ├── t_log_app_error_alarm_164.sql
    │   └── ...
    ├── mstream_alarm_19
    │   ├── t_log_ban_alarm_157.sql
    │   └── ...
    └── yid
```

# 监控

```shell
# 每小时检测一下application是否挂了
0 * * * * bash /data/flink-stream/flink-stream-sql-mctl monitor >/dev/null 2>&1
```

## 利用docker打包jar包

### 打包所有jar

```shell
docker run --rm -it -v  ~/.m2:/root/.m2 -v $(PWD):/www -w /www maven:3.6.3 mvn clean package
```

### 打包特定的jar包

```xml
<plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-jar-plugin</artifactId>
      <version>2.4</version><!--$NO-MVN-MAN-VER$-->
      <executions>
          <!-- Default Execution -->
          <execution>
              <id>default</id>
              <phase>package</phase>
              <goals>
                  <goal>test-jar</goal>
              </goals>
          </execution>

          <!-- 自定义序列化 -->
          <execution>
              <id>eventSerializer</id>
              <phase>package</phase>
              <goals>
                  <goal>jar</goal>
              </goals>
              <configuration>
                  <classifier>EventSerializer</classifier>

                  <archive>
                      <manifestEntries>
                          <program-class>org.client.flink.serializer.log_role_create.LogRoleCreateDeSerializer</program-class>
                      </manifestEntries>
                      <manifest>
                          <addClasspath>true</addClasspath>
                          <classpathPrefix>${project.build.directory}/dependency</classpathPrefix>
                      </manifest>
                  </archive>

                  <includes>
                      <include>org/client/flink/serializer/*/*.class</include>
                      <include>org/client/flink/serializer/*.class</include>
                      <include>org/client/flink/events/*/*.class</include>
                      <include>org/client/flink/events/*.class</include>
                      <include>META-INF/LICENSE</include>
                      <include>META-INF/NOTICE</include>
                  </includes>
              </configuration>
          </execution>

      </executions>
  </plugin>
```

### 打包指定jar，但是名称是统一的。没进行特殊化处理。

```shell
➜  flinkjob git:(master) ✗ docker run --rm -it -v  ~/.m2:/root/.m2 -v $(PWD):/www -w /www maven:3.6.3 mvn jar:jar@eventSerializer
[INFO] Scanning for projects...
[INFO]
[INFO] --------------------< org.client.flink:flinkjob >---------------------
[INFO] Building All client Flink job 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO]
[INFO] --- maven-jar-plugin:2.4:jar (EventSerializer) @ flinkjob ---
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  2.931 s
[INFO] Finished at: 2021-10-26T03:08:07Z
[INFO] ------------------------------------------------------------------------
```

### 指定最终的jar名称，这个全指定，没有任何的复用可言

```shell
➜  flinkjob git:(master) ✗ docker run --rm -it -v  ~/.m2:/root/.m2 -v $(PWD):/www -w /www maven:3.6.3 mvn jar:jar@eventSerializer -Djar.finalName=flinkjob-1.0-SNAPSHOT-Event
[INFO] Scanning for projects...
[INFO]
[INFO] --------------------< org.client.flink:flinkjob >---------------------
[INFO] Building All client Flink job 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO]
[INFO] --- maven-jar-plugin:2.4:jar (eventSerializer) @ flinkjob ---
[INFO] Building jar: /www/target/flinkjob-1.0-SNAPSHOT-Event.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  8.630 s
[INFO] Finished at: 2021-10-26T03:19:24Z
[INFO] ------------------------------------------------------------------------
```

### 需要根据现有的规则打包指定的jar

> 指定打包eventSerializer的jar，生成了对应的：mc-flinkjob-1.0-SNAPSHOT-eventSerializer.jar

```shell
➜  flinkjob git:(master) ✗ EXECUTE_ID=eventSerializer;BUILD_NAME=`docker run --rm -it -v  ~/.m2:/root/.m2 -v $(PWD):/www -w /www maven:3.6.3  mvn help:evaluate -Dexpression=project.build.finalName | egrep -v "^\[" | sed 's/^M//g'`;docker run --rm -it -v  ~/.m2:/root/.m2 -v $(PWD):/www -w /www maven:3.6.3 mvn jar:jar@${EXECUTE_ID} -Djar.finalName=${BUILD_NAME}-${EXECUTE_ID}
[INFO] Scanning for projects...
[INFO]
[INFO] --------------------< org.client.flink:flinkjob >---------------------
[INFO] Building All client Flink job 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO]
[INFO] --- maven-jar-plugin:2.4:jar (eventSerializer) @ flinkjob ---
[INFO] Building jar: /www/target/mc-flinkjob-1.0-SNAPSHOT-eventSerializer.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  8.550 s
[INFO] Finished at: 2021-10-26T06:09:54Z
[INFO] ------------------------------------------------------------------------
```
