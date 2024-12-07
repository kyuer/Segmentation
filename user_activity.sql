-- APP 表
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    user_name VARCHAR(100),
    register_date DATE,
    region VARCHAR(50),
    device_type VARCHAR(50),
    last_login TIMESTAMP,
    status VARCHAR(20)
);
CREATE TABLE user_events (
    event_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    event_type VARCHAR(50),
    event_time TIMESTAMP,
    duration INT,
    task_id BIGINT,
    payment_amount DECIMAL(10, 2),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE custom_campaigns (
    campaign_id BIGINT PRIMARY KEY,
    campaign_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    target_segment VARCHAR(50),
    reward_type VARCHAR(50)
);
CREATE TABLE campaign_records (
    record_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    campaign_id BIGINT,
    push_time TIMESTAMP,
    status VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (campaign_id) REFERENCES custom_campaigns(campaign_id)
);
CREATE TABLE user_activity (
    user_id VARCHAR(255) PRIMARY KEY, 
    login_times INT, 
    active_days INT, 
    total_payment FLOAT, 
    last_login_time TIMESTAMP, 
    tasks_completed INT, 
    churned BOOLEAN 
    -- 用户超过30天未登录或未支付可标记为流失
);

-- PB 宽表
CREATE EXTERNAL TABLE `default`.`user_behav`(
    `common` struct<product_name:string,module_name:string,logid:string,language:string,code_type:string,device_id:struct<imei:string,meid:string,mac_addr:string,cuid:string,pccode:string,open_udid:string,udid:string,idfa:string>,upload_timestamp:bigint,timezone:int,user_timestamp:bigint,user_timezone:int> COMMENT 'from deserializer' SAMPLE '',
    `public_info` struct<productline:string,package_name:string,software_version:string,platform:string,channel:string,current_channel:string,isbuiltin:int,isroot:int,phone_number:string,trace_time:struct<start_time:bigint,end_time:bigint>,os:string,os_version:string,os_info:string,manufacturer:string,terminal_brand:string,terminal_type:string,resolution_h:int,resolution_v:int,ppi:int,screen_size:string,cpu_info:string,mem_info:string,ip:string,bsinfo:string,net_type:string,operator_type:string,access_type:string,month_flow:int,data_size:int,bdinput_uid:string,collect_period:string,user_account:string> COMMENT 'from deserializer' SAMPLE '',
    `app_traces` array<struct<package_name:string,version_name:string,app_name:string,app_status:int,app_id:string,use_times:int,trace_logs:array<struct<start_time:bigint,end_time:bigint>>,use_seconds:bigint,panel_times:int,install_ts:bigint,app_change:string,url_box_times:int,url_box_enter_times:int,search_box_times:int,search_box_enter_times:int>> COMMENT 'from deserializer' SAMPLE '',
    `advertise_traces` array<struct<action_time:bigint,action_id:int,area_id:int,position_id:int,advertise_id:int,use_times:int,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `use_traces` array<struct<action_time:bigint,action_id:int,use_times:int,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `use_settings` array<struct<setting_id:int,setting_value:string>> COMMENT 'from deserializer' SAMPLE '',
    `log_type` int COMMENT 'from deserializer' SAMPLE '',
    `custom_logs` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `k8401` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `k8402` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `k8403` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `k8405` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `k8406` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `k8407` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '',
    `k8410` array<struct<action_time:bigint,action_ts:bigint,action_id:int,use_times:int,str_key:string,str_val:string>> COMMENT 'from deserializer' SAMPLE '')

COMMENT 'pb_user_behav_szth'
PARTITIONED BY (
    `event_day` string COMMENT '' SAMPLE '')
ROW FORMAT SERDE
'fileformat.orc.OrcSerDe'
STORED AS 
INPUTFORMAT
'mapred.OrcInputFormat'
OUTPUTFORMAT
'mapred.OrcOutputFormat'
LOCATION
'afs://XXX:9902/XXX_user_behav'
TBLPROPERTIES (
'computer'=''
,'bigdata_visible'='1'
,'description_filter'=''
,'DATA_MANAGEMENT'='SELF'
,'svn_address'='')

CREATE EXTERNAL TABLE `default`.`user_oaid_base`(
    `cuid` string COMMENT 'cuid' SAMPLE '',
    `oaid` string COMMENT 'oaid' SAMPLE '',
    `platform` string COMMENT '平台号' SAMPLE '',
    `last_login_time` string COMMENT '最近登录时间' SAMPLE '2024-11-05 23:42:30')
COMMENT 'oaid映射'
PARTITIONED BY (
    `event_day` string COMMENT '' SAMPLE '')
ROW FORMAT SERDE
'fileformat.orc.OrcSerDe'
STORED AS 
INPUTFORMAT
'mapred.OrcInputFormat'
OUTPUTFORMAT
'mapred.OrcOutputFormat'
LOCATION
'afs://XX:XX/XX_oaid_base'
TBLPROPERTIES (
'computer'=''
,'bigdata_visible'='1'
,'description_filter'=''
,'DATA_MANAGEMENT'='SELF'
,'svn_address'='')
