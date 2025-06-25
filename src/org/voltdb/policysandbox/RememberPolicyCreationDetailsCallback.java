package org.voltdb.policysandbox;

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

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/**
 * Special callback to update our PolicySession when we finish creating it 
 * with our assigned policy name and limit.
 *
 */
public class RememberPolicyCreationDetailsCallback implements ProcedureCallback {

    /**
     * The session this policy will be for.
     */
    PolicySession session;

    /**
     * Special callback to update our PolicySession when we finish creating it 
     * with our assigned policy name and limit.
     * @param PolicySession - our session
     *
     */
   public RememberPolicyCreationDetailsCallback(PolicySession session) {
        super();
        this.session = session;
    }

    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {

        if (arg0.getStatus() != ClientResponse.SUCCESS) {
            ConsoleMessageConsumer.msg("Error Code " + arg0.getStatusString());
        } else {

            VoltTable policyTable = arg0.getResults()[0];

            if (policyTable.advanceRow()) {
                synchronized (session) {
                    session.setPolicyNameAndLimit(policyTable.getString("policy_name"),
                            (int) policyTable.getLong("current_limit_per_user"));

                }
            }

        }

    }

}
