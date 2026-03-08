
-- 第一部分：数据清洗与预处理

-- 1.1 合并多个表去重 

CREATE TABLE project_until_2_24 AS
SELECT  DISTINCT *
FROM
(
    SELECT  *
    FROM project_until_2_23
    UNION ALL
    SELECT  *
    FROM project_2_24
) AS temp;

-- 1.2 基于指定列去重 

CREATE TABLE project_until_2_24_deduplication AS
SELECT  idfa
       ,event
       ,param
       ,time
FROM
(
    SELECT  *
           ,ROW_NUMBER() OVER (PARTITION BY idfa,event,time ORDER BY param ASC) AS rn
    FROM project_until_2_24
) t
WHERE rn = 1;

-- 1.3 参数值清洗：替换脏数据 

UPDATE project_until_2_24_deduplication
SET param = REPLACE(param, 'hello world', 'Hello_world'); 

-- 1.4 基础数据清洗：去除无效ID事件
-- 创建清洗后的数据表

CREATE TABLE project_until_2_24_clean AS
SELECT  *
FROM project_until_2_24_deduplication
WHERE idfa NOT IN ('00000000-0000-0000-0000-000000000000', 'unknown-adid', '0000-0000', 'unknow-adid');

-- 1.5 删除空param的重复数据 (重复拉取的数据中部分事件param丢失)
 DELETE
FROM project_until_2_24_clean
WHERE param = '{}'
AND (idfa, event, time) IN ( SELECT idfa, event, time FROM project_2_20 GROUP BY idfa, event, time HAVING COUNT(*) > 1);

-- 1.6 按时间截取数据 

CREATE TABLE project_2_20 AS
SELECT  *
FROM project_until_2_24_clean
WHERE date(time) = '2026-02-20';

-- 1.7 按事件拆分数据 

CREATE TABLE AD_Inter_Play AS
SELECT  *
FROM project_until_2_24_clean
WHERE event = 'event_1';

-- 1.8 数据质量评估：清洗前后对比 
 
WITH data_origin AS
(
    SELECT  '总事件数'       AS project
           ,COUNT(event) AS count
    FROM project_until_2_24
    UNION ALL
    SELECT  '总用户数'
           ,COUNT(DISTINCT idfa)
    FROM project_until_2_24
    UNION ALL
    SELECT  event
           ,COUNT(event)
    FROM project_until_2_24
    GROUP BY event
), data_clean AS
(
    SELECT  '总事件数'       AS project
           ,COUNT(event) AS count
    FROM project_until_2_24_clean
    UNION ALL
    SELECT  '总用户数'
           ,COUNT(DISTINCT idfa)
    FROM project_until_2_24_clean
    UNION ALL
    SELECT  event
           ,COUNT(event)
    FROM project_until_2_24_clean
    GROUP BY event
)
SELECT  a.project
       ,a.count                                            AS before_clean
       ,b.count                                            AS after_clean
       ,ROUND((a.count - b.count) * 1.0 / a.count * 100,2) AS invalid_pct
FROM data_origin a
JOIN data_clean b
ON a.project = b.project;

-- 1.9 按日期评估数据质量 
SELECT  t1.date
       ,t1.before_count
       ,t2.after_count
       ,ROUND((t1.before_count - t2.after_count) * 1.0 / t1.before_count * 100,2) AS invalid_pct
FROM
(
    SELECT  date(time) AS date
           ,COUNT(*)   AS before_count
    FROM project_until_2_24
    GROUP BY date
) t1
JOIN
(
    SELECT  date(time) AS date
           ,COUNT(*)   AS after_count
    FROM project_until_2_24_clean
    GROUP BY date
) t2
ON t1.date = t2.date;

-- 1.10 建立索引 
CREATE INDEX idx_event ON project_until_2_24_clean(event);
CREATE INDEX idx_user_time ON project_until_2_24_clean(idfa, time);
CREATE INDEX idx_event_time ON project_until_2_24_clean(event, time);

-- 第二部分：用户活跃与留存分析

-- 2.1 日活、滚动周活、滚动月活 
SELECT  date
       ,dau
       ,(
SELECT  COUNT(DISTINCT idfa)
FROM project_until_2_24_clean t2
WHERE DATE(t2.time) BETWEEN DATE(t1.date, '-6 days') AND t1.date) AS rolling_wau, (
SELECT  COUNT(DISTINCT idfa)
FROM project_until_2_24_clean t2
WHERE DATE(t2.time) BETWEEN DATE(t1.date, '-29 days') AND t1.date) AS rolling_mau, ROUND(dau * 1.0 / NULLIF((
SELECT  COUNT(DISTINCT idfa)
FROM project_until_2_24_clean t2
WHERE DATE(t2.time) BETWEEN DATE(t1.date, '-29 days') AND t1.date), 0), 4) AS dau_mau_ratio
FROM
(
    SELECT  DATE(time)           AS date
           ,COUNT(DISTINCT idfa) AS dau
    FROM project_until_2_24_clean
    GROUP BY DATE(time)
) t1
ORDER BY date DESC;

-- 2.2 周活（自然周）
SELECT  strftime('%Y-%W',time) AS year_week
       ,COUNT(DISTINCT idfa)   AS wau
FROM project_until_2_24_clean
GROUP BY strftime('%Y-%W',time)
ORDER BY year_week DESC;

-- 2.3 月活（自然月）
SELECT  strftime('%Y-%m',time) AS year_month
       ,COUNT(DISTINCT idfa)   AS mau
FROM project_until_2_24_clean
GROUP BY strftime('%Y-%m',time)
ORDER BY year_month DESC;

-- 2.4 新增用户数 
WITH first_occurrence AS
(
    SELECT  idfa
           ,DATE(MIN(time)) AS first_date
    FROM project_until_2_2
    GROUP BY idfa
)
SELECT  first_date  AS date
       ,COUNT(idfa) AS new_users
FROM first_occurrence
GROUP BY first_date
ORDER BY first_date;

-- 2.5 流失用户清单 - DuckDB
 DROP TABLE IF EXISTS user_churn;
CREATE OR REPLACE TABLE user_churn AS (
SELECT  idfa
       ,DATE(MAX(time)) AS last_date
FROM project_until_2_2
GROUP BY idfa
HAVING DATEDIFF('day', last_date::TIMESTAMP, NOW()) > 15);

-- 2.6 各小时活跃分布 
SELECT  strftime('%H',time)  AS hour_of_day
       ,COUNT(DISTINCT idfa) AS online_users
       ,COUNT(*)             AS total_events
FROM project_until_2_24_clean
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 2.7 留存表构建 - DuckDB 
 DROP TABLE IF EXISTS retention;
CREATE OR REPLACE TABLE retention AS (
WITH parsed_data AS
(
    SELECT  idfa
           ,event
           ,param
           ,strptime(time,'%Y-%m-%d %H:%M:%S')::DATE AS event_date
    FROM project_until_2_24_clean
), user_first_day AS
(
    SELECT  idfa
           ,MIN(event_date) AS install_date
    FROM parsed_data
    WHERE event = 'User_Open_First'
    GROUP BY idfa
), user_active_days AS
(
    SELECT  DISTINCT u.idfa
           ,u.install_date
           ,p.event_date                                AS active_date
           ,DATEDIFF('day',u.install_date,p.event_date) AS days_diff
    FROM user_first_day u
    INNER JOIN parsed_data p
    ON u.idfa = p.idfa AND p.event_date > u.install_date AND p.event_date <= u.install_date + INTERVAL '30 days' AND p.event IS NOT NULL AND p.event != 'User_Open_First'
)
SELECT  install_date                                                                                               AS cohort_date
       ,COUNT(DISTINCT idfa)                                                                                       AS cohort_size
       ,COUNT(DISTINCT CASE WHEN days_diff = 2 THEN idfa END)                                                      AS retained_day2
       ,ROUND(COUNT(DISTINCT CASE WHEN days_diff = 2 THEN idfa END) * 100.0 / GREATEST(COUNT(DISTINCT idfa),1),2)  AS retention_rate_day2_pct
       ,COUNT(DISTINCT CASE WHEN days_diff = 7 THEN idfa END)                                                      AS retained_day7
       ,ROUND(COUNT(DISTINCT CASE WHEN days_diff = 7 THEN idfa END) * 100.0 / GREATEST(COUNT(DISTINCT idfa),1),2)  AS retention_rate_day7_pct
       ,COUNT(DISTINCT CASE WHEN days_diff = 30 THEN idfa END)                                                     AS retained_day30
       ,ROUND(COUNT(DISTINCT CASE WHEN days_diff = 30 THEN idfa END) * 100.0 / GREATEST(COUNT(DISTINCT idfa),1),2) AS retention_rate_day30_pct
FROM user_active_days
GROUP BY install_date ORDER BY install_date);

-- 2.9 用户等级分层分析 
SELECT  AVG(Account)                                              AS avg_account
       ,AVG(User_count)                                           AS avg_round_count
       ,CASE WHEN Level <= 5 THEN 'a01-5'
             WHEN Level > 5 AND Level <= 10 THEN 'a6-10'
             WHEN Level > 10 AND Level <= 15 THEN 'b11-15'
             WHEN Level > 15 AND Level <= 20 THEN 'c16-20'
             WHEN Level > 20 AND Level <= 25 THEN 'd21-25'
             WHEN Level > 25 AND Level <= 30 THEN 'e26-30'
             WHEN Level > 30 AND Level <= 35 THEN 'f31-35'
             WHEN Level > 35 AND Level <= 40 THEN 'g36-40'
             WHEN Level > 40 AND Level <= 45 THEN 'h41-45'
             WHEN Level > 45 AND Level <= 50 THEN 'i46-50'
             WHEN Level > 50 THEN 'j50+'  ELSE 'unclassified' END AS user_level_group
FROM Round_Start
GROUP BY user_level_group
ORDER BY user_level_group;


-- 第三部分：会话行为分析

-- 3.1 事件间隔时间分布 
SELECT  (JULIANDAY(t2.time) - JULIANDAY(t1.time)) * 24 * 60 AS interval_minutes
FROM
(
    SELECT  idfa
           ,time
           ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
    FROM project_until_2_24_clean
) t1
JOIN
(
    SELECT  idfa
           ,time
           ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
    FROM project_until_2_24_clean
) t2
ON t1.rn = t2.rn - 1
WHERE t1.idfa = t2.idfa;

-- 3.2 事件间隔时间分布（聚合）- DuckDB
 DROP TABLE IF EXISTS interval_data;
CREATE OR REPLACE TABLE interval_data AS (
SELECT  interval
       ,COUNT(*)
FROM
(
    SELECT  CASE WHEN interval >= 60 THEN 60  ELSE interval END AS interval
    FROM
    (
        SELECT  DATEDIFF('minute',t1.time::TIMESTAMP,t2.time::TIMESTAMP) AS interval
        FROM
        (
            SELECT  idfa
                   ,time
                   ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
            FROM project_until_2_24_clean
        ) t1
        JOIN
        (
            SELECT  idfa
                   ,time
                   ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
            FROM project_until_2_24_clean
        ) t2
        ON t1.rn = t2.rn - 1
        WHERE t1.idfa = t2.idfa 
    )
)
GROUP BY interval ORDER BY interval);

-- 3.3 会话时长分布表 - DuckDB
 DROP TABLE IF EXISTS session_duration;
CREATE OR REPLACE TABLE session_duration AS (
WITH session AS
(
    SELECT  DISTINCT idfa
           ,event
           ,time
           ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
    FROM project_2_20
), session_side AS
(
    SELECT  idfa
           ,MIN(time) AS time
           ,'a_start' AS flag
    FROM session
    GROUP BY idfa
    UNION
    SELECT  t2.idfa
           ,t2.time
           ,'a_start' AS flag
    FROM session t1
    JOIN session t2
    ON t1.rn = t2.rn - 1 AND (t1.idfa != t2.idfa OR DATEDIFF('second', t1.time::TIMESTAMP, t2.time::TIMESTAMP) >= 600)
    UNION
    SELECT  t3.idfa
           ,t3.time
           ,'b_end' AS flag
    FROM session t3
    JOIN session t4
    ON t3.rn = t4.rn - 1 AND (t3.idfa != t4.idfa OR DATEDIFF('second', t3.time::TIMESTAMP, t4.time::TIMESTAMP) >= 600)
    UNION
    SELECT  idfa
           ,MAX(time) AS time
           ,'b_end'   AS flag
    FROM session
    GROUP BY idfa
), valid_session AS
(
    SELECT  t5.idfa
           ,t5.time                                                      AS start_time
           ,t6.time                                                      AS end_time
           ,DATEDIFF('minute',t5.time::TIMESTAMP,t6.time::TIMESTAMP) + 1 AS valid_duration
    FROM
    (
        SELECT  DISTINCT idfa
               ,time
               ,flag
               ,ROW_NUMBER() OVER (PARTITION BY idfa ORDER BY time,flag) AS rn
        FROM session_side
    ) t5
    JOIN
    (
        SELECT  DISTINCT idfa
               ,time
               ,flag
               ,ROW_NUMBER() OVER (PARTITION BY idfa ORDER BY time,flag) AS rn
        FROM session_side
    ) t6
    ON t5.idfa = t6.idfa AND t5.rn = t6.rn - 1 AND t5.flag = 'a_start' AND t6.flag = 'b_end' AND t5.time != t6.time
)
SELECT  valid_duration
       ,COUNT(*)
FROM valid_session
GROUP BY valid_duration ORDER BY valid_duration);

-- 3.4 退出游戏后3分钟无操作事件 
SELECT  t1.a_id    AS idfa
       ,t1.a_event AS event
       ,t1.a_param AS param
       ,t1.a_time  AS time
FROM
(
    SELECT  idfa                                    AS a_id
           ,event                                   AS a_event
           ,param                                   AS a_param
           ,time                                    AS a_time
           ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS a_rn
    FROM project
) t1
JOIN
(
    SELECT  t2.b_id
           ,t2.b_event
           ,t2.b_rn
    FROM
    (
        SELECT  idfa                                    AS b_id
               ,event                                   AS b_event
               ,time                                    AS b_time
               ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS b_rn
        FROM project
    ) t2
    JOIN
    (
        SELECT  event                                   AS c_event
               ,time                                    AS c_time
               ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS c_rn
        FROM project
    ) t3
    ON t2.b_rn = t3.c_rn - 1
    WHERE (JULIANDAY(t3.c_time) - JULIANDAY(t2.b_time)) * 24 * 60 > 3 
) t23
ON t1.a_rn = t23.b_rn - 1
WHERE t1.a_id = t23.b_id
AND t23.b_event = 'Exit_Game';

-- 3.5 退出会话最后事件分析 - DuckDB
 DROP TABLE IF EXISTS Last_event;
CREATE OR REPLACE TABLE Last_event AS (
WITH session AS
(
    SELECT  idfa
           ,time
           ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
    FROM project_until_2_24_clean
    WHERE event != 'Exit_Game' 
), session_side AS
(
    SELECT  idfa
           ,MIN(time) AS time
           ,'a_start' AS flag
    FROM session
    GROUP BY idfa
    UNION
    SELECT  t2.idfa
           ,t2.time
           ,'a_start' AS flag
    FROM session t1
    JOIN session t2
    ON t1.rn = t2.rn - 1 AND (t1.idfa != t2.idfa OR DATEDIFF('second', t1.time::TIMESTAMP, t2.time::TIMESTAMP) >= 600)
    UNION
    SELECT  t3.idfa
           ,t3.time
           ,'b_end' AS flag
    FROM session t3
    JOIN session t4
    ON t3.rn = t4.rn - 1 AND (t3.idfa != t4.idfa OR DATEDIFF('second', t3.time::TIMESTAMP, t4.time::TIMESTAMP) >= 600)
    UNION
    SELECT  idfa
           ,MAX(time) AS time
           ,'b_end'   AS flag
    FROM session
    GROUP BY idfa
), end_event AS
(
    SELECT  t6.idfa
           ,t6.time                                                      AS end_time
           ,DATEDIFF('minute',t5.time::TIMESTAMP,t6.time::TIMESTAMP) + 1 AS valid_duration
    FROM
    (
        SELECT  DISTINCT idfa
               ,time
               ,flag
               ,ROW_NUMBER() OVER (PARTITION BY idfa ORDER BY time,flag) AS rn
        FROM session_side
    ) t5
    JOIN
    (
        SELECT  DISTINCT idfa
               ,time
               ,flag
               ,ROW_NUMBER() OVER (PARTITION BY idfa ORDER BY time,flag) AS rn
        FROM session_side
    ) t6
    ON t5.idfa = t6.idfa AND t5.rn = t6.rn - 1 AND t5.flag = 'a_start' AND t6.flag = 'b_end' AND t5.time != t6.time
)
SELECT  t7.idfa
       ,t8.event
       ,t8.param
       ,t7.end_time
       ,t7.valid_duration
FROM end_event t7
INNER JOIN
(
    SELECT  *
    FROM project_until_2_24_clean
    WHERE event != 'Exit_Game' 
) t8
ON t7.idfa = t8.idfa AND t7.end_time = t8.time ORDER BY t7.idfa, t7.end_time);

-- 3.6 退出会话最后事件分布 
SELECT  event
       ,COUNT() AS count
       ,ROUND(COUNT() * 100.0 / (
SELECT  COUNT(*)
FROM Last_event), 2) AS percentage
FROM Last_event
GROUP BY event
ORDER BY percentage DESC;


-- 第四部分：游戏回合行为分析

-- 4.1 游戏回合时长分布表 - DuckDB
 DROP TABLE IF EXISTS round_duration;
CREATE OR REPLACE TABLE round_duration AS (
WITH rounds AS
(
    SELECT  DISTINCT idfa
           ,event
           ,time
           ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
    FROM project_2_20
    WHERE event IN ('Round_Result', 'Round_Start') 
), valid_rounds AS
(
    SELECT  t1.idfa
           ,t1.time                                                  AS start_time
           ,t2.time                                                  AS end_time
           ,DATEDIFF('second',t1.time::TIMESTAMP,t2.time::TIMESTAMP) AS valid_duration
    FROM rounds t1
    JOIN rounds t2
    ON t1.rn = t2.rn - 1 AND t1.idfa = t2.idfa AND t1.event = 'Round_Start' AND t2.event = 'Round_Result'
    WHERE valid_duration < 300 
)
SELECT  valid_duration
       ,COUNT(*)
FROM valid_rounds
GROUP BY valid_duration ORDER BY valid_duration);

-- 4.2 游戏回合总体分析 
WITH valid_rounds AS
(
    SELECT  t1.idfa
           ,t1.time                                                  AS start_time
           ,t2.time                                                  AS end_time
           ,(JULIANDAY(t2.time) - JULIANDAY(t1.time)) * 24 * 60 * 60 AS valid_duration
    FROM
    (
        SELECT  idfa
               ,event
               ,time
               ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
        FROM project_2_20
        WHERE event IN ('Round_Result', 'Round_Start') 
    ) t1
    JOIN
    (
        SELECT  idfa
               ,event
               ,time
               ,ROW_NUMBER() OVER (ORDER BY idfa,time) AS rn
        FROM project_2_20
        WHERE event IN ('Round_Result', 'Round_Start') 
    ) t2
    ON t1.rn = t2.rn - 1 AND t1.idfa = t2.idfa AND t1.event = 'Round_Start' AND t2.event = 'Round_Result'
    WHERE valid_duration < 300 
)
SELECT  '游戏回合数'   AS title
       ,COUNT() AS value
FROM valid_rounds
UNION
SELECT  '完成游戏回合人数'
       ,COUNT(DISTINCT idfa)
FROM valid_rounds
UNION
SELECT  '人均游戏回合数'
       ,ROUND(COUNT() * 1.0 / COUNT(DISTINCT idfa),2)
FROM valid_rounds
ORDER BY title;

-- 4.3 分场景胜率分析 
CREATE INDEX idx_round_result_scene ON Round_Result(scene);
CREATE INDEX idx_round_result_user ON Round_Result(idfa);
CREATE INDEX idx_round_result_streak ON Round_Result(Streak_win);

SELECT  scene
       ,R.win * 1.0 / R.Round_count            AS win_rate
       ,R.win * 1.0 / (R.Round_count - R.push) AS not_push_win_rate
       ,R.push * 1.0 / R.Round_count           AS push_rate
FROM
(
    SELECT  scene
           ,SUM(CASE WHEN Result IN ('BJ_WIN','WIN') THEN 1 ELSE 0 END) AS win
           ,SUM(CASE WHEN Result = 'PUSH' THEN 1 ELSE 0 END)            AS push
           ,COUNT(Result)                                               AS Round_count
    FROM Round_Result
    GROUP BY scene
) AS R;

-- 4.4 总胜率 
SELECT  R.win * 1.0 / R.Round_count            AS win_rate
       ,R.win * 1.0 / (R.Round_count - R.push) AS not_push_win_rate
       ,R.push * 1.0 / R.Round_count           AS push_rate
FROM
(
    SELECT  SUM(CASE WHEN Result IN ('BJ_WIN','WIN') THEN 1 ELSE 0 END) AS win
           ,SUM(CASE WHEN Result = 'PUSH' THEN 1 ELSE 0 END)            AS push
           ,COUNT(Result)                                               AS Round_count
    FROM Round_Result
) AS R;

-- 4.5 游戏回合结果分布 
SELECT  Result
       ,COUNT(*) AS count
FROM Round_Result
GROUP BY Result;

-- 4.6 连胜场次分布 
SELECT  Streak_win
       ,COUNT(*) AS count
FROM Round_Result
GROUP BY Streak_win;

-- 4.7 投入值与持有货币比值分布 
WITH all_round AS
(
    SELECT  Bet * 1.0 / Account                                                 AS ratio
           ,CAST(FLOOR(Bet * 1.0 / Account * 10 - 0.0000000001) AS INTEGER)     AS bucket
    FROM Round_Result
    WHERE Account IS NOT NULL
    AND Account >= Bet 
), lose_round AS
(
    SELECT  scene
           ,Bet * 1.0 / Account                                                 AS ratio
           ,CAST(FLOOR(Bet * 1.0 / Account * 10 - 0.0000000001) AS INTEGER)     AS bucket
    FROM Round_Result
    WHERE Result IN ('LOSE', 'BJ_LOSE', 'PLAYER_BUST')
    AND Account IS NOT NULL
    AND Account >= Bet 
)
-- 所有游戏回合
SELECT  'ALL'   AS scene
       ,bucket
       ,COUNT() AS count
       ,COUNT() * 1.0 / (
SELECT  COUNT(*)
FROM all_round) AS percentage
FROM all_round
WHERE bucket != 0
GROUP BY bucket
UNION ALL
-- 失败游戏回合
SELECT  'ALL_Lose' AS scene
       ,bucket
       ,COUNT(*)   AS count
       ,COUNT(*) * 1.0 / (
SELECT  COUNT(*)
FROM lose_round) AS percentage
FROM lose_round
WHERE bucket != 0
GROUP BY bucket
UNION ALL
-- 各场景失败游戏回合
SELECT  scene
       ,bucket
       ,COUNT() AS count
       ,COUNT() * 1.0 / (
SELECT  COUNT(*)
FROM lose_round a
WHERE a.scene = b.scene) AS percentage
FROM lose_round b
WHERE bucket != 0
GROUP BY scene
         ,bucket
ORDER BY scene
         ,bucket;

-- 4.8 投入值与场景最低投入值比值分布 
WITH round_info AS
(
    SELECT  scene
           ,Bet
           ,Result
           ,CASE WHEN scene = 'scene_1' THEN 5
                 WHEN scene = 'scene_2' THEN 250
                 WHEN scene = 'scene_3' THEN 5000
                 WHEN scene = 'scene_4' THEN 25000
                 WHEN scene = 'scene_5' THEN 50000  ELSE 0 END AS base_bet
    FROM Round_Result
), all_round AS
(
    SELECT  scene
           ,Bet * 1.0 / base_bet                                                                 AS ratio
           ,CASE WHEN Bet * 1.0 / base_bet BETWEEN 1 AND 10 THEN 'a1-10'
                 WHEN Bet * 1.0 / base_bet BETWEEN 11 AND 50 THEN 'b11-50'
                 WHEN Bet * 1.0 / base_bet BETWEEN 51 AND 100 THEN 'c51-100'
                 WHEN Bet * 1.0 / base_bet BETWEEN 101 AND 200 THEN 'd101-200'  ELSE 'e200+' END AS bucket
    FROM round_info
    WHERE Bet >= base_bet 
), lose_round AS
(
    SELECT  scene
           ,Bet * 1.0 / base_bet                                                                 AS ratio
           ,CASE WHEN Bet * 1.0 / base_bet BETWEEN 1 AND 10 THEN 'a1-10'
                 WHEN Bet * 1.0 / base_bet BETWEEN 11 AND 50 THEN 'b11-50'
                 WHEN Bet * 1.0 / base_bet BETWEEN 51 AND 100 THEN 'c51-100'
                 WHEN Bet * 1.0 / base_bet BETWEEN 101 AND 200 THEN 'd101-200'  ELSE 'e200+' END AS bucket
    FROM round_info
    WHERE Result IN ('LOSE', 'BJ_LOSE', 'PLAYER_BUST')
    AND Bet >= base_bet 
)
-- 所有游戏回合
SELECT  'ALL'   AS scene
       ,bucket
       ,COUNT() AS count
       ,COUNT() * 1.0 / (
SELECT  COUNT()
FROM all_round) AS percentage
FROM all_round
GROUP BY bucket
UNION ALL
-- 失败游戏回合
SELECT  'ALL_Lose' AS scene
       ,bucket
       ,COUNT()    AS count
       ,COUNT(*) * 1.0 / (
SELECT  COUNT(*)
FROM lose_round) AS percentage
FROM lose_round
GROUP BY bucket
UNION ALL
-- 各场景失败游戏回合
SELECT  scene
       ,bucket
       ,COUNT() AS count
       ,COUNT() * 1.0 / (
SELECT  COUNT(*)
FROM lose_round a
WHERE a.scene = b.scene) AS percentage
FROM lose_round b
GROUP BY scene
         ,bucket
ORDER BY scene;


-- 第五部分：广告效果分析

-- 5.1 全屏广告完播率 
SELECT  1 - (
SELECT  COUNT(event)
FROM last_event_AD_Inter_Play) * 1.0 / (
SELECT  COUNT(event)
FROM AD_Inter_Play) AS fullscreen_ad_completion_rate;

-- 5.2 播放全屏广告的概率 
SELECT  (
SELECT  COUNT(event)
FROM AD_Inter_Play) * 1.0 / (
SELECT  COUNT(event)
FROM Round_Result) AS fullscreen_ad_play_rate;

-- 5.3 播放激励视频人数占比 
SELECT  (
SELECT  COUNT(DISTINCT idfa)
FROM AD_Reward_Play) * 1.0 / (
SELECT  COUNT(DISTINCT idfa)
FROM project_until_2_24_clean) AS reward_ad_player_pct;

-- 5.4 至少完播一次激励视频人数占比 
SELECT  (
SELECT  COUNT(DISTINCT idfa)
FROM AD_Reward_Complete) * 1.0 / (
SELECT  COUNT(DISTINCT idfa)
FROM AD_Reward_Play) AS reward_ad_completer_pct;

-- 5.5 激励视频完播率 
SELECT  (
SELECT  COUNT(event)
FROM AD_Reward_Complete) * 1.0 / (
SELECT  COUNT(event)
FROM AD_Reward_Play) AS reward_ad_completion_rate;

-- 5.6 单个玩家单日触发激励视频次数分析 
SELECT  play_count
       ,COUNT(play_count) AS player_count
       ,SUM(play_count)   AS total_plays
FROM
(
    SELECT  idfa
           ,COUNT(*)   AS play_count
           ,date(time) AS day
    FROM AD_Reward_Play
    GROUP BY idfa
             ,day
)
GROUP BY play_count;

-- 5.7 各场景触发激励视频次数 
SELECT `where` 
       ,COUNT(event) AS count
FROM
(
    SELECT  DISTINCT idfa
           ,event
           ,`where`
    FROM AD_Reward_Play
) GROUP BY `where`
ORDER BY count DESC;

-- 5.8 触发激励视频场景分析（完播率） 
SELECT  a.`where`
       ,b.count * 1.0 / a.count AS completion_rate
FROM
(
    SELECT `where` 
           ,COUNT(event) AS count
    FROM
    (
        SELECT  DISTINCT idfa
               ,event
               ,`where`
        FROM AD_Reward_Play
    ) GROUP BY `where` 
) a
JOIN
(
    SELECT `where` 
           ,COUNT(event) AS count
    FROM
    (
        SELECT  DISTINCT idfa
               ,event
               ,`where`
        FROM AD_Reward_Complete
    ) GROUP BY `where` 
) b
ON a.`where` = b.`where`;


-- 第六部分：按钮点击行为分析

-- 6.1 Button点击表构建 
 DROP TABLE IF EXISTS Button;
CREATE TABLE Button AS
SELECT  COALESCE(t1.idfa,t2.idfa,t3.idfa,t4.idfa,t5.idfa,t10.idfa) AS idfa
       ,COALESCE(t1.button_a_count,0)                              AS button_a_count
       ,COALESCE(t2.button_b_count,0)                              AS button_b_count
       ,COALESCE(t3.button_c_count,0)                              AS button_c_count
       ,COALESCE(t4.button_d_count,0)                              AS button_d_count
       ,COALESCE(t5.button_e_count,0)                              AS button_e_count
       ,t10.Account
       ,t10.Level
       ,t10.User_count
FROM
(
    SELECT  idfa
           ,MAX(COUNT) AS button_a_count
    FROM button_a_Click
    GROUP BY idfa
) t1
FULL JOIN
(
    SELECT  idfa
           ,MAX(COUNT) AS button_b_count
    FROM button_b_Click
    GROUP BY idfa
) t2
ON t1.idfa = t2.idfa
FULL JOIN
(
    SELECT  idfa
           ,MAX(COUNT) AS button_c_count
    FROM button_c_Click
    GROUP BY idfa
) t3
ON COALESCE(t1.idfa, t2.idfa) = t3.idfa
FULL JOIN
(
    SELECT  idfa
           ,MAX(COUNT) AS button_d_count
    FROM button_d_Click
    GROUP BY idfa
) t4
ON COALESCE(t1.idfa, t2.idfa, t3.idfa) = t4.idfa
FULL JOIN
(
    SELECT  idfa
           ,MAX(COUNT) AS button_e_count
    FROM button_e_Click
    GROUP BY idfa
) t5
ON COALESCE(t1.idfa, t2.idfa, t3.idfa, t4.idfa) = t5.idfa
RIGHT JOIN
(
    SELECT  idfa
           ,Account
           ,Level
           ,User_count
    FROM Round_Start
    GROUP BY idfa
    HAVING time = MAX(time)
) t10
ON COALESCE(t1.idfa, t2.idfa, t3.idfa, t4.idfa, t5.idfa) = t10.idfa
WHERE t1.button_a_count IS NOT NULL OR t2.button_b_count IS NOT NULL OR t3.button_c_count IS NOT NULL OR t4.button_d_count IS NOT NULL OR t5.button_e_count IS NOT NULL;

-- 6.2 局内交互按钮点击次数随等级变化趋势 
SELECT  level
       ,AVG(Account)        AS avg_account
       ,AVG(user_count)     AS avg_round_count
       ,AVG(button_a_count) AS avg_button_a_count
       ,AVG(button_b_count) AS avg_button_b_count
       ,AVG(button_c_count) AS avg_button_c_count
       ,AVG(button_d_count) AS avg_button_d_count
       ,AVG(button_e_count) AS avg_button_e_count
FROM Button
WHERE level <= 300
AND Account < 10000000
GROUP BY level
HAVING COUNT(level) >= 5;

-- 6.3 至少播放一次激励视频玩家行为 
SELECT  level
       ,AVG(Account)        AS avg_account
       ,AVG(user_count)     AS avg_round_count
       ,AVG(button_a_count) AS avg_button_a_count
       ,AVG(button_b_count) AS avg_button_b_count
       ,AVG(button_c_count) AS avg_button_c_count
       ,AVG(button_d_count) AS avg_button_d_count
       ,AVG(button_e_count) AS avg_button_e_count
FROM
(
    SELECT  DISTINCT b.*
    FROM
    (
        SELECT  DISTINCT idfa
        FROM AD_Reward_Play
    ) a
    JOIN Button b
    ON a.idfa = b.idfa
)
GROUP BY level;

-- 6.4 至少完播一次激励视频玩家行为 
SELECT  level
       ,AVG(Account)        AS avg_account
       ,AVG(user_count)     AS avg_round_count
       ,AVG(button_a_count) AS avg_button_a_count
       ,AVG(button_b_count) AS avg_button_b_count
       ,AVG(button_c_count) AS avg_button_c_count
       ,AVG(button_d_count) AS avg_button_d_count
       ,AVG(button_e_count) AS avg_button_e_count
FROM
(
    SELECT  DISTINCT b.*
    FROM
    (
        SELECT  DISTINCT idfa
        FROM AD_Reward_Complete
    ) a
    JOIN Button b
    ON a.idfa = b.idfa
)
GROUP BY level;

-- 6.5 至少跳过一次全屏广告的玩家行为 
SELECT  level
       ,AVG(Account)        AS avg_account
       ,AVG(user_count)     AS avg_round_count
       ,AVG(button_a_count) AS avg_button_a_count
       ,AVG(button_b_count) AS avg_button_b_count
       ,AVG(button_c_count) AS avg_button_c_count
       ,AVG(button_d_count) AS avg_button_d_count
       ,AVG(button_e_count) AS avg_button_e_count
FROM
(
    SELECT  DISTINCT b.*
    FROM
    (
        SELECT  DISTINCT idfa
        FROM last_event_AD_Inter_Play
    ) a
    JOIN Button b
    ON a.idfa = b.idfa
)
GROUP BY level;

-- 6.6 每50局聚合更新分段 
UPDATE Round_Result

SET Round_count_section = CASE 
    WHEN User_count IS NULL THEN '未知' 
    WHEN User_count < 1 THEN '<1' 
    WHEN User_count > 500 THEN '>500' 
    ELSE (((User_count - 1) / 50) * 50 + 1) || '-' || (((User_count - 1) / 50) * 50 + 50) END;

