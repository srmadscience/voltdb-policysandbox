
DROP TASK ChangePoliciesTask;

DROP PROCEDURE ShowPolicyStatus__promBL IF EXISTS;

DROP PROCEDURE findbusycells IF EXISTS;

DROP PROCEDURE ReportNewSession IF EXISTS;
   
DROP PROCEDURE ReportEndSession IF EXISTS;
   
DROP PROCEDURE ReportSessionUsage IF EXISTS;
   
DROP PROCEDURE ChangePolicies IF EXISTS;

DROP TOPIC console_messages;

DROP TOPIC policy_change_session_messages;

DROP VIEW session_policy_cell_users;

DROP VIEW cell_activity_summary;

DROP TABLE policy_parameters ;

DROP TABLE available_policies;

DROP TABLE session_policy_state;
     
DROP TABLE policy_active_limits_by_cell;
     
DROP STREAM cell_activity;

DROP STREAM policy_change_session_messages;

DROP STREAM console_messages;





   






