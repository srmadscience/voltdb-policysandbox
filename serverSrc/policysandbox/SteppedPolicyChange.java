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
public class SteppedPolicyChange extends VoltProcedure {

    // @formatter:off
    
       
    public static final SQLStmt findNextChange 
    = new SQLStmt("SELECT MIN(policy_change_started)  policy_change_started "
            + "FROM policy_active_limits_by_cell "
            + "WHERE policy_change_started IS NOT NULL;");

  
    public static final SQLStmt getChange 
    = new SQLStmt("SELECT policy_change_percent_done,cell_id,policy_name"
            + ", current_limit_per_user "
            + "FROM policy_active_limits_by_cell "
            + "WHERE policy_change_started = ? "
            + "ORDER BY cell_id,policy_name "
            + "LIMIT 1;");
  
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
                    + "AND   policy_name = ? "
                    + "AND   mod(sessionId,100) = ?; ");
    
    public static final SQLStmt updateStatus 
    = new SQLStmt("UPDATE policy_active_limits_by_cell "
            + "SET policy_change_percent_done = ? "
            + "WHERE cell_id = ? "
            + "AND   policy_name = ?;");
  
 
    public static final SQLStmt finishTask 
    = new SQLStmt("UPDATE policy_active_limits_by_cell "
            + "SET policy_change_percent_done = null "
            + "  , policy_change_started = null "
            + "WHERE cell_id = ? "
            + "AND   policy_name = ?;");               

    public static final SQLStmt sendMessageToConsole = new SQLStmt(
            "INSERT INTO console_messages  " +
                    "(thing_id,message_date,message_text) VALUES " +
                     "(?,NOW,?);");
   

    // @formatter:on

    public VoltTable[] run() throws VoltAbortException {

        voltQueueSQL(findNextChange);
        VoltTable nextChangeExistsTable = voltExecuteSQL()[0];

        nextChangeExistsTable.advanceRow();
        TimestampType nextChangeTimestamp = nextChangeExistsTable.getTimestampAsTimestamp("policy_change_started");

        if (nextChangeTimestamp != null) {

            voltQueueSQL(getChange, nextChangeTimestamp);
            VoltTable changeTable = voltExecuteSQL()[0];

            changeTable.advanceRow();

            String policyName = changeTable.getString("policy_name");
            long cellId = changeTable.getLong("cell_id");
            byte pctDone = (byte) changeTable.getLong("policy_change_percent_done");
            long newLimit = changeTable.getLong("current_limit_per_user");

            pctDone++;

            voltQueueSQL(sendMessageToDevice, newLimit, cellId, policyName, pctDone);

            if (pctDone < 99) {
                voltQueueSQL(updateStatus, pctDone, cellId, policyName);
                voltQueueSQL(sendMessageToConsole, cellId,
                        "Cell/Policy change @" + nextChangeTimestamp.toString() + " " + pctDone + "% done.");
            } else {
                voltQueueSQL(finishTask, cellId, policyName);
                voltQueueSQL(sendMessageToConsole, cellId,
                        "Cell/Policy change @" + nextChangeTimestamp.toString() + " finished.");
            }

        }

        return voltExecuteSQL(true);

    }

}
