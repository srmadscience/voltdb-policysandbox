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
* This redistributes  capacity within a cell
 *
 */
public class ChangeCellAllocation extends VoltProcedure {

    // @formatter:off
    
    public static final SQLStmt getPolicyAndCell = new SQLStmt(
            "SELECT * "
            + "FROM policy_active_limits_by_cell "
            + "WHERE policy_name = ? "
            + "AND   cell_id = ?; ");

    public static final SQLStmt updatePolicyAndCell = new SQLStmt(
            "UPDATE policy_active_limits_by_cell "
            + "SET current_limit_per_cell = current_limit_per_cell + ? "
            + "  , last_update_date = NOW "
            + "WHERE policy_name = ? "
            + "AND   cell_id = ?; ");

    
    public static final SQLStmt sendMessageToConsole = new SQLStmt(
            "INSERT INTO console_messages  " +
                    "(thing_id,message_date,message_text) VALUES " +
                     "(?,NOW,?);");
 
    public static final SQLStmt getCellPolicies = new SQLStmt(
            "SELECT CELL_ID, POLICY_NAME, CURRENT_LIMIT_PER_CELL, CURRENT_LIMIT_PER_USER "
            + "FROM policy_active_limits_by_cell "
            + "WHERE cell_id = ?"
            + "ORDER BY policy_name; ");


    
    
   
    // @formatter:on

    /**
     * Move capacity between policies in cell
     * @param cellId
     * @param fromPolicy
     * @param toPolicy
     * @param amount
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(long cellId, String fromPolicy, String toPolicy, long amount) throws VoltAbortException {

        voltQueueSQL(getPolicyAndCell,EXPECT_ONE_ROW, fromPolicy,cellId);
        voltQueueSQL(getPolicyAndCell,EXPECT_ONE_ROW,toPolicy,cellId);

        VoltTable[] queryResults = voltExecuteSQL();

        VoltTable fromPolicyResult = queryResults[0];
        
        @SuppressWarnings("unused")
        VoltTable toPolicyResult = queryResults[1];
        
        fromPolicyResult.advanceRow();
        long currentFromLimit = fromPolicyResult.getLong("current_limit_per_cell");
        
        if (currentFromLimit < amount) {
            voltQueueSQL(sendMessageToConsole,cellId,"Current 'from' is " + currentFromLimit
                    + ". Can't reduce by " + amount);
        } else {
            voltQueueSQL(updatePolicyAndCell,(amount * -1), fromPolicy,cellId);
            voltQueueSQL(updatePolicyAndCell,amount , toPolicy,cellId);
            voltQueueSQL(sendMessageToConsole,cellId,"Current 'from' is " + currentFromLimit
                    + ". Reduced by " + amount);
        }
        
        voltQueueSQL(getCellPolicies, cellId);
        
        return voltExecuteSQL(true);

    }

   
}
