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