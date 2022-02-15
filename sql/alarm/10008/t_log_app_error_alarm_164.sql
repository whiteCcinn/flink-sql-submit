-- {"headers":{"app_id":10008,"log_name":"app_error","client_ip":"139.9.200.216","mtime":1639047096},"logs":{"app_id":0,"child_app":"baseservice","client_ip":"","detail":"","invalid_col_1":"","ip":"10.8.18.99","level":2,"mdate":"2021-12-08","mtime":1640073599,"pid":1468896115125391381,"related_app_id":0,"request_id":0,"summary":"[unexpect_error] \u003creporter get config error\u003e: game: zzfxd, platform: mcfx, channelvia: 100757"}}

-- {
--     "app_ids": [
--         10008
--     ],
--     "log_name": "app_error",
--     "executors": [
--         {
--             "id": 164,
--             "cycle_type": [
--                 "minute",
--                 1
--             ],
--             "type": "count",
--             "group_field": [
--                 "related_app_id",
--                 "child_app",
--                 "summary",
--                 "level",
--                 "ip"
--             ],
--             "calculate_field": "detail",
--             "mdate_field_name": "mdate",
--             "mtime_field_name": "mtime",
--             "result_field_name": "cnt",
--             "store": {
--                 "target_db_module": "mstore_mysql",
--                 "target_db_name": "db_app_log_alarm",
--                 "target_table_name": "t_log_app_error_alarm_164"
--             }
--         }
--     ],
--     "enabled": true
-- }

-- 以":"为分隔符，分别代表：catalog_type, hive_conf_path, catalog_name
-- "-" 代表使用默认值
CATALOG_INFO = hive:/opt/hadoopclient/Hive/config/:-;

CREATE DATABASE mstream_alarm COMMENT '告警系统流计算';

USE mstream_alarm;

SET 'pipeline.name' = '每1分钟基础服务告警';
SET 'table.exec.emit.early-fire.enabled' = 'true';
SET 'table.exec.emit.early-fire.delay' = '10s';
SET 'mc.local.time.zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'drop';
-- checkpoint配置
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '2min';
SET 'execution.checkpointing.timeout' = '1min';
SET 'execution.checkpointing.prefer-checkpoint-for-recovery' = true;
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'mc.state.backend.fs.checkpointdir' = 'hdfs:///flink/checkpoints/{db}/{pipeline.name}';
SET 'mc.execution.savepoint.dir' = 'hdfs:///flink/savepoints/{db}/{pipeline.name}';
-- 重启策略
SET 'restart-strategy' = 'failure-rate';
SET 'restart-strategy.failure-rate.delay' = '10s';
SET 'restart-strategy.failure-rate.failure-rate-interval' = '5min';
SET 'restart-strategy.failure-rate.max-failures-per-interval' = '10';

CREATE TABLE app_error_To_t_log_app_error_alarm_164 (
    headers ROW<`app_id` int,`log_name` string>,
    logs ROW<`related_app_id` int, `child_app` varchar(200), `summary` string,`level` int,`ip` varchar(200),`detail` varchar(100), `mtime` int>,
    etime as TO_TIMESTAMP(FROM_UNIXTIME(logs.`mtime`)),
    WATERMARK for etime AS etime -- defines watermark on ts column, marks ts as event-time attribute
)
WITH (
    'connector' = 'kafka',
    'topic' = 'mfeilog_dsp_10008_app_error',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'app_error_to_t_log_app_error_alarm_164',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE `t_log_app_error_alarm_164` (
  `related_app_id` int,
  `child_app` varchar(200),
  `summary` string,
  `level` int,
  `ip` varchar(200) ,
  `cnt` varchar(200) COMMENT 'calculate the detail of count()',
  `mdate` string,
  `mtime` int,
  PRIMARY KEY (`related_app_id`,`child_app`,`summary`,`level`,`ip`) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:60701/db_app_log_alarm?useUnicode=true&characterEncoding=utf8&autoReconnect=true',
   'driver' = 'com.mysql.cj.jdbc.Driver',
   'table-name' = 't_log_app_error_alarm_164',
   'username' = 'flink_mstream_alarm',
   'password' = 'xxxx'
);

insert into t_log_app_error_alarm_164 (
    select t1.`related_app_id`,t1.`child_app`,t1.`summary`,t1.`level`,t1.`ip`,cast(t1.`cnt` as VARCHAR(200)) as `cnt`,t1.`mdate`,cast (t1.`mtime` as INT)  from (
        select
            logs.`related_app_id` as `related_app_id`,
            logs.`child_app` as `child_app`,
            logs.`summary` as `summary`,
            logs.`level` as `level`,
            logs.`ip` as `ip`,
            DATE_FORMAT(TUMBLE_START(etime, INTERVAL '1' MINUTE), 'yyyy-MM-dd') as `mdate`,
            UNIX_TIMESTAMP(DATE_FORMAT(TUMBLE_START(etime, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss')) as `mtime`,
            COUNT(logs.`detail`) as `cnt`
        FROM app_error_To_t_log_app_error_alarm_164
        GROUP BY logs.`related_app_id`, logs.`child_app`,logs.`summary`,logs.`level`,logs.`ip`,TUMBLE(etime, INTERVAL '1' MINUTE)
    ) t1
);
