
DROP TASK ChangePoliciesTask IF EXISTS;

DROP TASK SteppedPolicyChangeTask IF EXISTS;

DROP PROCEDURE ShowPolicyStatus__promBL IF EXISTS;

DROP PROCEDURE findbusycells IF EXISTS;

DROP PROCEDURE ReportNewSession IF EXISTS;
   
DROP PROCEDURE ReportEndSession IF EXISTS;
   
DROP PROCEDURE ReportSessionUsage IF EXISTS;
   
DROP PROCEDURE ChangePolicies IF EXISTS;

DROP PROCEDURE SteppedPolicyChange IF EXISTS;

DROP PROCEDURE ChangeCellAllocation IF EXISTS;

DROP TOPIC console_messages IF EXISTS;

DROP TOPIC policy_change_session_messages IF EXISTS;

DROP VIEW session_policy_cell_users IF EXISTS;

DROP VIEW policy_change_counts IF EXISTS;

DROP VIEW cell_activity_summary IF EXISTS;

DROP TABLE policy_parameters  IF EXISTS;

DROP TABLE available_policies IF EXISTS;

DROP TABLE session_policy_state IF EXISTS;
     
DROP TABLE policy_active_limits_by_cell IF EXISTS;
     
DROP STREAM cell_activity IF EXISTS;

DROP STREAM policy_change_session_messages IF EXISTS;

DROP STREAM console_messages IF EXISTS;





   






