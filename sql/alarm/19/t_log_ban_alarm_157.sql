-- {"headers":{"app_id":19,"client_ip":"106.75.150.228","log_time":1639789591,"log_name":"log_ban"},"logs":{"account_name":"1639789574001003497","agent_id":6,"app_package_type":0,"auto":0,"category":4,"cid":"","client_version":"","create_time":1639789589,"device":"HUAWEI CDY-TN00","device_id":"","entrance_agent_id":6,"entrance_server_id":134,"faction_id":0,"idfa":"","imei":"","ip":"120.243.186.57","is_retran":0,"mac":"","mdate":"2021-12-18","mtime":1639789589,"os":"","phone_system":"Android OS 10 / API-29 (HUAWEICDY-TN00/102.0.0.211C01)","pid":1639789589000020,"platform":303,"regrow":0,"role_id":60134100010700,"role_name":"羽又蓝","server_id":134,"server_version":"","sex":2,"upf":1,"via":"6|303"}}

-- 以":"为分隔符，分别代表：catalog_type, hive_conf_path, catalog_name
-- "-" 代表使用默认值
CATALOG_INFO = hive:/opt/hadoopclient/Hive/config/:-;

CREATE DATABASE mstream_alarm COMMENT '告警系统流计算';

USE mstream_alarm;

SET 'pipeline.name' = '每30分钟封禁玩家数量';
SET 'table.exec.emit.early-fire.enabled' = 'true';
SET 'table.exec.emit.early-fire.delay' = '1800s';
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


CREATE TABLE log_ban_To_t_log_ban_alarm_157 (
    headers ROW<`app_id` int,`log_name` string>,
    logs ROW<`ban_key` string, `mtime` int>,
    etime as TO_TIMESTAMP(FROM_UNIXTIME(CAST(logs.`mtime` as BIGINT))),
    WATERMARK for etime AS etime -- defines watermark on ts column, marks ts as event-time attribute
)
WITH (
    'connector' = 'kafka',
    'topic' = 'mfeilog_dsp_19_log_ban',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'log_ban_To_t_log_ban_alarm_157',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE `t_log_ban_alarm_157` (
  `count_ban_key` bigint COMMENT 'calculate the real_name of count()',
  `mdate` varchar(50),
  `mtime` int,
  PRIMARY KEY (`mtime`) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:60701/db_zzfx_log_alarm?useUnicode=true&characterEncoding=utf8&autoReconnect=true',
   'driver' = 'com.mysql.cj.jdbc.Driver',
   'table-name' = 't_log_ban_alarm_157',
   'username' = 'flink_mstream_alarm',
   'password' = 'xxxx'
);

insert into t_log_ban_alarm_157 (
    select t1.`count_ban_key`,t1.`mdate`,cast (t1.`mtime` as INT)  from (
        select
            DATE_FORMAT(TUMBLE_START(etime, INTERVAL '30' MINUTE), 'yyyy-MM-dd') as `mdate`,
            UNIX_TIMESTAMP(DATE_FORMAT(TUMBLE_START(etime, INTERVAL '30' MINUTE), 'yyyy-MM-dd HH:mm:ss')) as `mtime`,
            COUNT(logs.`ban_key`) as `count_ban_key`
        FROM log_ban_To_t_log_ban_alarm_157
        GROUP BY TUMBLE(etime, INTERVAL '30' MINUTE)
    ) t1
);