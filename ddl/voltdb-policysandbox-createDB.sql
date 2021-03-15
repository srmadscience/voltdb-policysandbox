
LOAD CLASSES ../jars/voltdb-policysandbox.jar;

file -inlinebatch END_OF_BATCH

--
-- Various parameters used to control system behavior
--
CREATE TABLE policy_parameters 
(parameter_name varchar(30) not null primary key
,parameter_value bigint not null);

--
-- Available policies
--
CREATE TABLE available_policies
(policy_name  varchar(30) not null primary key
,policy_max_bandwidth_per_min bigint not null
,policy_min_bandwidth_per_min bigint not null
,policy_start_range  bigint not null
,policy_end_range  bigint not null
);

-- 
-- What policy a given session has, what cell it is in,
-- and when we last talked to it
--
CREATE TABLE session_policy_state
(	 sessionId bigint not null,
	 sessionStartUTC timestamp not null,
	 cell_id bigint not null,
	 policy_name varchar(30) not null,
	 last_policy_update_date timestamp not null,
	 primary key (sessionId,sessionStartUTC,cell_id)
)
USING TTL 25 HOURS ON COLUMN last_policy_update_date BATCH_SIZE 50000;

PARTITION TABLE session_policy_state ON COLUMN cell_id;

CREATE INDEX sps_ix1 ON session_policy_state (last_policy_update_date);

CREATE INDEX sps_ix2 ON session_policy_state (cell_id,last_policy_update_date);

CREATE INDEX sps_ix3 ON session_policy_state (cell_id,policy_name,last_policy_update_date);

--
-- View to track stale  session_policy_state records
--
CREATE VIEW session_policy_cell_users AS
SELECT cell_id, policy_name
     , MIN(last_policy_update_date) last_policy_update_date
     , count(*) active_users
FROM session_policy_state
GROUP BY cell_id,policy_name;



--
-- Stream to record usage by session.
--
CREATE STREAM cell_activity PARTITION ON COLUMN cell_id 
(cell_id bigint not null 
,sessionId bigint not null
,sessionStartUTC timestamp not null
,policy_name  varchar(30) not null 
,usage_amount bigint not null 
,usage_timestamp timestamp not null);

--
-- View that calculates usage by policy per cell
--
CREATE VIEW cell_activity_summary AS
SELECT cell_id
     , TRUNCATE(MINUTE, usage_timestamp) usage_timestamp
     , policy_name 
     , sum(usage_amount) total_usage_amount
     , max(usage_amount) max_per_user_usage_amount
     , count(*) how_many 
FROM cell_activity
GROUP BY cell_id
     , TRUNCATE(MINUTE, usage_timestamp) 
     , policy_name;
     
CREATE INDEX cas_ix1 ON cell_activity_summary(usage_timestamp);

--
-- Table tracking actual, current limits by cell
--
CREATE TABLE policy_active_limits_by_cell
(cell_id bigint not null 
,policy_name  varchar(30) not null 
,current_limit_per_cell  bigint not null 
,current_limit_per_user  bigint not null 
,last_update_date timestamp not null
,primary key (cell_id,policy_name));
     
PARTITION TABLE policy_active_limits_by_cell ON COLUMN cell_id;


--
-- Stream so we can tell sessions to limit
-- their activity
--
CREATE STREAM policy_change_session_messages PARTITION ON COLUMN cell_id 
(sessionId bigint not null
,sessionStartUTC timestamp not null
,changeTimestamp timestamp not null
,cell_id bigint not null 
,new_limit bigint not null);

CREATE TOPIC USING STREAM policy_change_session_messages PROFILE daily;

CREATE STREAM console_messages PARTITION ON COLUMN thing_id 
(thing_id bigint not null 
,message_date timestamp not null
,message_text varchar(180) not null);

CREATE TOPIC USING STREAM console_messages PROFILE daily;

DROP PROCEDURE findbusycells IF EXISTS;

create procedure findbusycells AS
SELECT cas.cell_id
               , cas.policy_name
               , cas.total_usage_amount 
               , palbc.current_limit_per_cell 
               , palbc.current_limit_per_user
               , cas.total_usage_amount / spcu.active_users  average_per_user
               , cas.max_per_user_usage_amount
               , spcu.active_users
               , cas.how_many
               , ap.policy_max_bandwidth_per_min
               , ap.policy_min_bandwidth_per_min   
               , (cas.total_usage_amount * 100)/  palbc.current_limit_per_cell  cell_pct_full
     FROM   cell_activity_summary cas    
        ,   policy_active_limits_by_cell palbc  
        ,   available_policies ap    
        ,   session_policy_cell_users spcu
     WHERE palbc.policy_name = ap.policy_name    
     AND   spcu.cell_id = palbc.cell_id 
     AND   spcu.policy_name = palbc.policy_name 
     AND   cas.cell_id = palbc.cell_id    
     AND   cas.policy_name = palbc.policy_name    
     AND   cas.usage_timestamp = TRUNCATE(MINUTE, DATEADD(MINUTE,-1,NOW))    
     ORDER BY cas.cell_id, cas.policy_name, cas.total_usage_amount, palbc.current_limit_per_user ; 

DROP PROCEDURE ReportNewSession IF EXISTS;

CREATE PROCEDURE  
   PARTITION ON TABLE session_policy_state COLUMN cell_id
   FROM CLASS policysandbox.ReportNewSession;  
   
DROP PROCEDURE ReportEndSession IF EXISTS;

CREATE PROCEDURE  
   PARTITION ON TABLE session_policy_state COLUMN cell_id
   FROM CLASS policysandbox.ReportEndSession;  
   
DROP PROCEDURE ReportSessionUsage IF EXISTS;

CREATE PROCEDURE  
   PARTITION ON TABLE session_policy_state COLUMN cell_id
   FROM CLASS policysandbox.ReportSessionUsage;  
   
DROP PROCEDURE ChangePolicies IF EXISTS;

CREATE PROCEDURE DIRECTED
   FROM CLASS policysandbox.ChangePolicies;  
   
CREATE TASK ChangePoliciesTask
ON SCHEDULE  EVERY 60 SECONDS
PROCEDURE ChangePolicies
ON ERROR LOG 
RUN ON PARTITIONS;
   

CREATE PROCEDURE ShowPolicyStatus__promBL AS
BEGIN
--
select 'policy_parameter'||parameter_name statname
     ,  'policy_parameter_'||parameter_name stathelp  
     , parameter_value statvalue 
from policy_parameters order by parameter_name;
--
SELECT 'cell_activity_cell_pct_full' statname
     , 'cell_activity_cell_pct_full' stathelp
               , cas.cell_id
               , cas.policy_name
               , (cas.total_usage_amount * 100)/  palbc.current_limit_per_cell  cell_pct_full
       FROM   cell_activity_summary cas  
          ,   policy_active_limits_by_cell palbc
      WHERE cas.usage_timestamp = TRUNCATE(MINUTE, DATEADD(MINUTE,-1,NOW))    
      AND  cas.policy_name = palbc.policy_name    
     AND   cas.cell_id = palbc.cell_id 
     ORDER BY cas.cell_id, cas.policy_name ; 
--
SELECT 'cell_activity_average_per_user' statname
     , 'cell_activity_average_per_user' stathelp
               , cas.cell_id
               , cas.policy_name
               , cas.total_usage_amount / spcu.active_users   statvalue
       FROM   cell_activity_summary cas  
          ,   session_policy_cell_users spcu  
      WHERE cas.usage_timestamp = TRUNCATE(MINUTE, DATEADD(MINUTE,-1,NOW))    
      AND  cas.policy_name = spcu.policy_name    
     AND   cas.cell_id = spcu.cell_id    
     ORDER BY cas.cell_id, cas.policy_name ; 
--
SELECT 'policy_max_bandwidth_per_min' statname
     , 'policy_max_bandwidth_per_min' stathelp
               , ap.policy_name
               , ap.policy_max_bandwidth_per_min   statvalue
       FROM   available_policies ap
       ORDER BY ap.policy_name ; 
--    
SELECT 'policy_min_bandwidth_per_min' statname
     , 'policy_min_bandwidth_per_min' stathelp
               , ap.policy_name
               , ap.policy_min_bandwidth_per_min   statvalue
       FROM   available_policies ap
       ORDER BY ap.policy_name ; 
--
SELECT 'cell_activity_total_usage_amount' statname
     , 'cell_activity_total_usage_amount' stathelp
               , cas.cell_id
               , cas.policy_name
               , cas.total_usage_amount statvalue
       FROM   cell_activity_summary cas    
      WHERE cas.usage_timestamp = TRUNCATE(MINUTE, DATEADD(MINUTE,-1,NOW))    
     ORDER BY cas.cell_id, cas.policy_name ; 
--
SELECT 'cell_activity_current_limit_per_cell' statname
     , 'cell_activity_current_limit_per_cell' stathelp
               , palbc.cell_id
               , palbc.policy_name
               , palbc.current_limit_per_cell statvalue
       FROM   policy_active_limits_by_cell palbc
     ORDER BY palbc.cell_id, palbc.policy_name ; 
--
SELECT 'cell_activity_current_limit_per_user' statname
     , 'cell_activity_current_limit_per_user' stathelp
               , palbc.cell_id
               , palbc.policy_name
               , palbc.current_limit_per_user statvalue
       FROM   policy_active_limits_by_cell palbc
     ORDER BY palbc.cell_id, palbc.policy_name ; 
--
END;

END_OF_BATCH


upsert into policy_parameters
(parameter_name ,parameter_value)
VALUES
('ENABLE_POLICY_ENFORCEMENT',1);

upsert into policy_parameters
(parameter_name ,parameter_value)
VALUES
('SHRINK_PCT',75);

upsert into policy_parameters
(parameter_name ,parameter_value)
VALUES
('GROW_PCT',105);

upsert into policy_parameters
(parameter_name ,parameter_value)
VALUES
('PANIC_SHRINK_PCT',200);

upsert into policy_parameters
(parameter_name ,parameter_value)
VALUES
('USER_CELL_QUOTA_PCT',2);

upsert into policy_parameters
(parameter_name ,parameter_value)
VALUES
('DEFAULT_CELL_TOTAL_CAPACITY',2000000000);



UPSERT INTO available_policies
(policy_name, policy_max_bandwidth_per_min, policy_min_bandwidth_per_min
,policy_start_range,policy_end_range)
VALUES
('NONE',2000,100,-1,-1);

UPSERT INTO available_policies
(policy_name, policy_max_bandwidth_per_min, policy_min_bandwidth_per_min
,policy_start_range,policy_end_range)
VALUES
('EXECUTIVE_PLUS',2000000000,2000,0,10000);

UPSERT INTO available_policies
(policy_name, policy_max_bandwidth_per_min, policy_min_bandwidth_per_min
,policy_start_range,policy_end_range)
VALUES
('AVERAGE_USER',1000000000,10000,10001,100000);

UPSERT INTO available_policies
(policy_name, policy_max_bandwidth_per_min, policy_min_bandwidth_per_min
,policy_start_range,policy_end_range)
VALUES
('STARVING_STUDENT',500000000,10,100001,999999999);
