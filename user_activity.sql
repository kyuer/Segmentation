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