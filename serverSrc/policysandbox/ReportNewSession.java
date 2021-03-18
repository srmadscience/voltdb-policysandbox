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

import java.util.Date;

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
import org.voltdb.VoltType;

/**
 * Create a session at the start of the run
 *
 */
public class ReportNewSession extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getParameter = new SQLStmt(
            "SELECT parameter_value FROM policy_parameters WHERE parameter_name = ? ;");
       
    public static final SQLStmt getPolicy = new SQLStmt(
            "SELECT policy_name, policy_max_bandwidth_per_min "
            + "FROM available_policies "
            + "WHERE ? BETWEEN policy_start_range AND policy_end_range "
            + "ORDER BY policy_max_bandwidth_per_min DESC, policy_name; ");


    public static final SQLStmt getPolicyLimitsByCell = new SQLStmt(
            "SELECT current_limit_per_user FROM policy_active_limits_by_cell "
            + "WHERE cell_id = ? "
            + "AND policy_name = ?; ");


    public static final SQLStmt createNewSession = new SQLStmt(
            "INSERT INTO session_policy_state  " +
                    "( sessionId, " +
                    " sessionStartUTC, " +
                    " cell_id, " +
                    " policy_name, " +
                    " last_policy_update_date)  " +
                    "VALUES " +
                    "(?,?,?,?,NOW); ");

    public static final SQLStmt createNewPolicyLimitsByCell = new SQLStmt(
            "INSERT INTO policy_active_limits_by_cell  " +
                    "(cell_id, " +
                    " policy_name," +
                    "current_limit_per_cell, " +
                     "current_limit_per_user, " +
                     " last_update_date)  " +
                    "VALUES " +
                    "(?,?,?,?,NOW); ");

    public static final SQLStmt sendMessageToConsole = new SQLStmt(
            "INSERT INTO console_messages  " +
                    "(thing_id,message_date,message_text) VALUES " +
                     "(?,NOW,?);");


		
	// @formatter:on

    public VoltTable[] run(long cellId, long sessionId, Date sessionStartUTC, long userId) throws VoltAbortException {

        String policyName = "";
        long defaultCellTotalCapacity = 1000000;
        long userCellFractionOf = 1000;
        long userCellCapacityPerUser = 0;

        voltQueueSQL(getPolicy, EXPECT_ONE_ROW, userId);
        VoltTable policyResults = voltExecuteSQL()[0];
        policyResults.advanceRow();
        policyName = policyResults.getString("policy_name");

        voltQueueSQL(getPolicyLimitsByCell, cellId, policyName);
        voltQueueSQL(getParameter, "USER_CELL_FRACTION_OF");
        voltQueueSQL(getParameter, "DEFAULT_CELL_TOTAL_CAPACITY");

        VoltTable[] policyLimits = voltExecuteSQL();

        VoltTable currentPolicyTableForThisCell = policyLimits[0];
        VoltTable userCellFractionTable = policyLimits[1];
        VoltTable defaultCellTotalCapacityTable = policyLimits[2];

        if (!currentPolicyTableForThisCell.advanceRow()) {

            defaultCellTotalCapacity = getParameter(defaultCellTotalCapacity, defaultCellTotalCapacityTable);
            userCellFractionOf = getParameter(userCellFractionOf, userCellFractionTable);
            userCellCapacityPerUser = defaultCellTotalCapacity / userCellFractionOf;

            long maxBandwidthPerUser = policyResults.getLong("policy_max_bandwidth_per_min");

            if (userCellCapacityPerUser < maxBandwidthPerUser) {
                maxBandwidthPerUser = userCellCapacityPerUser;
            }

            voltQueueSQL(createNewPolicyLimitsByCell, cellId, policyName, defaultCellTotalCapacity,
                    maxBandwidthPerUser);
            voltQueueSQL(sendMessageToConsole, cellId,
                    "Created new Cell/Policy " + cellId + "/" + policyName + " with limit of " + maxBandwidthPerUser);

        } else {
            userCellCapacityPerUser = currentPolicyTableForThisCell.getLong("current_limit_per_user");
        }

        voltQueueSQL(createNewSession, sessionId, sessionStartUTC, cellId, policyName);
        voltExecuteSQL(true);

        VoltTable resultTable = new VoltTable(new VoltTable.ColumnInfo("policy_name", VoltType.STRING),
                new VoltTable.ColumnInfo("current_limit_per_user", VoltType.BIGINT));
        resultTable.addRow(policyName, userCellCapacityPerUser);

        VoltTable[] voltTableArray = { resultTable };

        return (voltTableArray);

    }

    protected long getParameter(long value, VoltTable parameterTable) {
        if (parameterTable.advanceRow()) {
            value = parameterTable.getLong("parameter_value");
        }
        return value;
    }

}
