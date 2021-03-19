package policysandbox;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * This runs on each partition as a DIRECTED PROCEDURE and is kicked off by a
 * TASK.
 * 
 * It finds cells that are busy are adjusts quotas downward in increments of
 * SHRINK_PCT
 *
 */
public class ChangePolicies extends VoltProcedure {

    // @formatter:off
    
    public static final SQLStmt getParameter = new SQLStmt(
            "SELECT parameter_value FROM policy_parameters WHERE parameter_name = ? ;");
       
    public static final SQLStmt findOverloadedCells 
    = new SQLStmt("SELECT cas.cell_id, cas.policy_name, cas.total_usage_amount"
            + ", cas.max_per_user_usage_amount, palbc.current_limit_per_user"
            + ", spcu.active_users, "
            + "cas.total_usage_amount /spcu.active_users  average_per_user,"
            + "ap.policy_max_bandwidth_per_min, ap.policy_min_bandwidth_per_min, "
            + "(cas.total_usage_amount * 100)/  palbc.current_limit_per_cell  cell_pct_full " +
    "FROM   cell_activity_summary cas " +
    "   ,   policy_active_limits_by_cell palbc "
    + " ,   available_policies ap "
    + " ,   session_policy_cell_users spcu " +
    "WHERE palbc.policy_name = ap.policy_name " +
    "AND   cas.cell_id = palbc.cell_id " +
    "AND   cas.policy_name = palbc.policy_name " +
    "AND   spcu.cell_id = palbc.cell_id " +
    "AND   spcu.policy_name = palbc.policy_name " +
    "AND   cas.usage_timestamp = TRUNCATE(MINUTE, DATEADD(MINUTE,-1,NOW)) " +
    "AND   cas.total_usage_amount > palbc.current_limit_per_cell " +
    "AND   palbc.last_update_date  < TRUNCATE(MINUTE, DATEADD(MINUTE,-2,NOW)) " +
    "AND   palbc.policy_change_started  IS NULL " +
    "ORDER BY cas.cell_id, cas.policy_name, cas.total_usage_amount, palbc.current_limit_per_user " +
    "LIMIT 20; ");

    public static final SQLStmt findUnderloadedCells 
    = new SQLStmt("SELECT cas.cell_id, cas.policy_name, cas.total_usage_amount"
            + ", cas.max_per_user_usage_amount, palbc.current_limit_per_user"
            + ", spcu.active_users, "
            + "cas.total_usage_amount /spcu.active_users  average_per_user,"
            + "ap.policy_max_bandwidth_per_min, ap.policy_min_bandwidth_per_min, "
            + "(cas.total_usage_amount * 100)/  palbc.current_limit_per_cell  cell_pct_full " +
    "FROM   cell_activity_summary cas " +
    "   ,   policy_active_limits_by_cell palbc "
    + " ,   available_policies ap "
    + " ,   session_policy_cell_users spcu " +
    "WHERE palbc.policy_name = ap.policy_name " +
    "AND   cas.cell_id = palbc.cell_id " +
    "AND   cas.policy_name = palbc.policy_name " +
    "AND   spcu.cell_id = palbc.cell_id " +
    "AND   spcu.policy_name = palbc.policy_name " +
    "AND   cas.usage_timestamp = TRUNCATE(MINUTE, DATEADD(MINUTE,-1,NOW)) " +
    "AND   (cas.total_usage_amount * 1.1) < palbc.current_limit_per_cell " +
    "AND   palbc.last_update_date  < TRUNCATE(MINUTE, DATEADD(MINUTE,-2,NOW)) " +
    "AND   palbc.policy_change_started  IS NULL " +
   "ORDER BY cas.cell_id, cas.policy_name, cas.total_usage_amount, palbc.current_limit_per_user " +
    "LIMIT 20; ");

    public static final SQLStmt updateCellLimit = new SQLStmt(
            "UPDATE policy_active_limits_by_cell "
            + "SET current_limit_per_user = ?"
            + "  , last_update_date = NOW "
            + "WHERE cell_id = ? "
            + "AND   policy_name = ?;");
  
    public static final SQLStmt startSteppedUpdate = new SQLStmt(
            "UPDATE policy_active_limits_by_cell "
            + "SET policy_change_percent_done = 0"
            + "  , policy_change_started = NOW "
            + "WHERE cell_id = ? "
            + "AND   policy_name = ?;");
 
    public static final SQLStmt sendMessageToDevice = new SQLStmt(
            "INSERT INTO policy_change_session_messages  " +
                    "(sessionId, " +
                    " sessionStartUTC,"
                    + "changeTimestamp, " +
                    " cell_id, "
                    + " new_limit)  " +
                    "SELECT sessionId, "
                    + " sessionStartUTC, "
                    + " NOW, "
                    + " cell_id, CAST(? AS BIGINT) "
                    + "FROM session_policy_state "
                    + "WHERE cell_id = ? "
                    + "AND   policy_name = ?; ");
                   // + "ORDER BY sessionId,sessionStartUTC,cell_id;");

    public static final SQLStmt sendMessageToConsole = new SQLStmt(
            "INSERT INTO console_messages  " +
                    "(thing_id,message_date,message_text) VALUES " +
                     "(?,NOW,?);");
   

    // @formatter:on

    public VoltTable[] run() throws VoltAbortException {

        long growPct = 5;
        long shrinkPct = 0;
        long panicShrinkPct = 1000;
        boolean enablePolicy = false;
        long maxSessonsPerChange = 100000;

        voltQueueSQL(getParameter, "ENABLE_POLICY_ENFORCEMENT");
        voltQueueSQL(getParameter, "MIN_SHRINK_PCT");
        voltQueueSQL(getParameter, "PANIC_SHRINK_PCT");
        voltQueueSQL(getParameter, "MIN_GROW_PCT");
        voltQueueSQL(getParameter, "MAX_SESSIONS_PER_SINGLE_CHANGE");
        voltQueueSQL(findOverloadedCells);
        voltQueueSQL(findUnderloadedCells);

        VoltTable[] queryResults = voltExecuteSQL();

        VoltTable enablePolicyResult = queryResults[0];
        VoltTable shrinkPctResult = queryResults[1];
        VoltTable panicShrinkPctResult = queryResults[2];
        VoltTable growPctResult = queryResults[3];
        VoltTable maxSessonsPerChangeResult = queryResults[4];

        enablePolicy = getParameter(enablePolicy, enablePolicyResult);
        shrinkPct = getParameter(shrinkPct, shrinkPctResult);
        panicShrinkPct = getParameter(panicShrinkPct, panicShrinkPctResult);
        growPct = getParameter(growPct, growPctResult);
        maxSessonsPerChange = getParameter(maxSessonsPerChange, maxSessonsPerChangeResult);

        VoltTable overloadedCellResult = queryResults[5];
        VoltTable underloadedCellResult = queryResults[6];

        while (overloadedCellResult.advanceRow()) {

            final long cellId = overloadedCellResult.getLong("cell_id");
            final String policyName = overloadedCellResult.getString("policy_name");
            final long userCount = overloadedCellResult.getLong("active_users");
            final long averageAmountPerUser = overloadedCellResult.getLong("average_per_user");
            final long currentLimitPerUser = overloadedCellResult.getLong("current_limit_per_user");
            final long cellPctFull = overloadedCellResult.getLong("cell_pct_full");
            final long minBandwidthPerMin = overloadedCellResult.getLong("policy_min_bandwidth_per_min");

            String event;
            long targetLimitPerUser;

            if (cellPctFull > panicShrinkPct) {

                event = "PanicShrink";
                targetLimitPerUser = (long) ((currentLimitPerUser * panicShrinkPct) / cellPctFull);

            } else {
                
                event = "Shrink:";

                long pctAdjust = (cellPctFull - 100) / 2;
                
                if (pctAdjust < shrinkPct) {
                    pctAdjust = shrinkPct;
                }
                             
                targetLimitPerUser = (currentLimitPerUser * (100 - pctAdjust)) / 100;
                
            }

            if (targetLimitPerUser < minBandwidthPerMin) {
                
                event = "ShrinkHitLimit";
                targetLimitPerUser = minBandwidthPerMin;
            }

            if (enablePolicy && currentLimitPerUser != targetLimitPerUser) {

                voltQueueSQL(sendMessageToConsole, cellId,
                        "ChangePolicies: " + event + ": Cell/policy " + cellId + "/" + policyName + " is at "
                                + cellPctFull + "%. Shrinking from " + currentLimitPerUser + " to "
                                + targetLimitPerUser + " for " + userCount + " users. Current Avg is "
                                + averageAmountPerUser);
                voltQueueSQL(updateCellLimit, targetLimitPerUser, cellId, policyName);

                if (maxSessonsPerChange < userCount) {
                    voltQueueSQL(startSteppedUpdate, cellId, policyName);
                } else {
                    voltQueueSQL(sendMessageToDevice, targetLimitPerUser, cellId, policyName);
                }

            }

            voltExecuteSQL();

        }

        while (underloadedCellResult.advanceRow()) {

            final long cellId = underloadedCellResult.getLong("cell_id");
            final String policyName = underloadedCellResult.getString("policy_name");
            final long userCount = underloadedCellResult.getLong("active_users");
            final long averageAmountPerUser = underloadedCellResult.getLong("average_per_user");
            final long currentLimitPerUser = underloadedCellResult.getLong("current_limit_per_user");
            final long cellPctFull = underloadedCellResult.getLong("cell_pct_full");
            final long maxBandwidthPerMin = underloadedCellResult.getLong("policy_max_bandwidth_per_min");

            String event;
            long targetLimitPerUser;

            event = "Grow:";
            
            long pctAdjust = ((cellPctFull - 100) / 2) * -1;
            
            if (pctAdjust < growPct) {
                pctAdjust = growPct;
            }
                         
            targetLimitPerUser = (currentLimitPerUser * (100 + pctAdjust)) / 100;

            // fix bug: If your growPct <= <10 and GROW_PCT is 105 you won't get an
            // increase.
            // Force one...
            if (targetLimitPerUser <= currentLimitPerUser) {
                targetLimitPerUser++;
            }

            if (targetLimitPerUser > maxBandwidthPerMin) {
                event = "GrowHitLimit";
                targetLimitPerUser = maxBandwidthPerMin;
            }

            if (enablePolicy) {

                voltQueueSQL(sendMessageToConsole, cellId, "ChangePolicies: " + event + ": Cell/policy " + cellId + "/"
                        + policyName + " is at " + cellPctFull + "%. Growing by " + pctAdjust + "% from " + currentLimitPerUser + " to "
                        + targetLimitPerUser + " for " + userCount + " users. Current Avg is " + averageAmountPerUser);
                voltQueueSQL(updateCellLimit, targetLimitPerUser, cellId, policyName);

                if (maxSessonsPerChange < userCount) {
                    voltQueueSQL(startSteppedUpdate, cellId, policyName);
                } else {
                    voltQueueSQL(sendMessageToDevice, targetLimitPerUser, cellId, policyName);
                }

            }

            voltExecuteSQL();

        }

        return voltExecuteSQL(true);

    }

    private boolean getParameter(boolean enablePolicy, VoltTable enablePolicyResult) {
        if (enablePolicyResult.advanceRow()) {
            if (enablePolicyResult.getLong("parameter_value") == 1) {
                enablePolicy = true;
            } else {
                enablePolicy = false;
            }
        }
        return enablePolicy;
    }

    protected long getParameter(long value, VoltTable parameterTable) {
        if (parameterTable.advanceRow()) {
            value = parameterTable.getLong("parameter_value");
        }
        return value;
    }

}
