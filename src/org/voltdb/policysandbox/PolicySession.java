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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

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

import java.util.Random;

import org.voltdb.types.TimestampType;

/**
 * Class representing a user session in our policy demo.
 *
 */
public class PolicySession {

    /**
     * We only generate one non-zero record per minute per session as part of our
     * simulation.
     */
    private static final long ONE_MINUTE_MS = 60000;

    /**
     * Last time we generated a message. Defaults to 1970.
     */
    private long lastMessageTime = 0;

    /**
     * The user this session is for. A user can have multiple sessions.
     */
    private long userId;

    /**
     * A session is identified by sessionId + sessionStartUTC
     */
    private long sessionId;

    /**
     * A session is identified by sessionId + sessionStartUTC
     */
    private TimestampType sessionStartUTC;

    /**
     * Id of 'cell' session is using
     */
    private long cellId;

    /**
     * Because we expect to have millions of PolicySessions at the same time we
     * share an instance of Random.
     */
    Random r;

    /**
     * How much bandwidth we are allowed to use per minute.
     */

    private long usageLimit = 1;

    /**
     * Which policy we have. This will be updated by the Callback to
     * ReportNewSessiom, but its possible for usage to be generated before we get
     * the callback..
     */
    private String policyName = "NONE";

    /**
     * We store a list of messages so we can understand what's happened to this
     * session during the run.
     */
    ArrayList<String> policyChangeMessages = new ArrayList<String>();

    /**
     * Used for formatting change message timestamps
     */
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Whether we remember messages or not...
     */
    boolean remember = false;

    /**
     * Create one of millions of Policy Sessions we keeop track of.
     * 
     * @param sessionStartUTC
     * @param sessionid
     * @param userId
     * @param cellId
     * @param r
     */
    public PolicySession(TimestampType sessionStartUTC, long sessionid, long userId, long cellId, Random r) {
        super();
        this.sessionStartUTC = sessionStartUTC;
        this.sessionId = sessionid;
        this.userId = userId;
        this.cellId = cellId;
        this.r = r;
    }

    /**
     * Get a usage message. Will be for zero if we have already done so within the
     * last minute.
     * 
     * @return A new usage message
     */
    public PolicyUsageMessage getNextUsageMessage() {

        long usage = 0;

        if (lastMessageTime + ONE_MINUTE_MS < System.currentTimeMillis()) {
            usage = r.nextInt((int) usageLimit);
            lastMessageTime = System.currentTimeMillis();
        }

        PolicyUsageMessage newMessage = new PolicyUsageMessage(cellId, sessionId, sessionStartUTC, policyName, usage);

        if (usage > 0) {
            msg("Reported Usage of " + newMessage.getRecordUsage());

        }
        return newMessage;
    }

    /**
     * @return the usageLimit
     */
    public long getUsageLimit() {
        return usageLimit;
    }

    /**
     * @param usageLimit the usageLimit to set
     */
    public void setUsageLimit(int usageLimit) {
        this.usageLimit = usageLimit;
        msg("Limit changed to " + usageLimit);
    }

    /**
     * @return the policyName
     */
    public String getPolicyName() {
        return policyName;
    }

    /**
     * @param policyName the policyName to set
     */
    public void setPolicyName(String policyName) {
        this.policyName = policyName;
        msg("Policy changed to " + policyName);
    }

    /**
     * Print a formatted message.
     * 
     * @param message
     */
    private void msg(String message) {

        if (remember) {
            Date now = new Date();
            String strDate = sdfDate.format(now);
            policyChangeMessages.add(strDate + ":" + message);
        }

    }

    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();
        builder.append(sessionId);
        builder.append(",");
        builder.append(sessionStartUTC);
        builder.append(",");
        builder.append(userId);
        builder.append(",");
        builder.append(cellId);
        builder.append(",[");

        Iterator<String> iterator = policyChangeMessages.iterator();

        while (iterator.hasNext()) {
            String nextObject = iterator.next();
            builder.append(nextObject);
            builder.append(',');
            builder.append(System.lineSeparator());

        }

        builder.append("]");

        return builder.toString();
    }

    /**
     * @return An Object[] containing the fields in the correct order used by the
     *         VoltDB procedures ReportNewSession and ReportEndSession
     */
    public Object[] getParamsForVoltDBCall() {

        Object[] newParams = new Object[4];

        newParams[0] = cellId;
        newParams[1] = sessionId;
        newParams[2] = sessionStartUTC.asExactJavaDate();
        newParams[3] = userId;

        return newParams;

    }

    /**
     * Update our session with a new limit. This will have arrived via Kafka..
     * 
     * @param policyChangeMessage
     */
    public void changePolicy(PolicyChangeMessage policyChangeMessage) {

        this.usageLimit = policyChangeMessage.getNewLimit();

        msg("Policy limit changed to " + usageLimit);

    }

    /**
     * Update our session with a new policy name and limit. This will have arrived
     * via VoltDB..
     * 
     * @param policyName
     * @param usageLimit
     */
    public void setPolicyNameAndLimit(String policyName, long usageLimit) {

        this.policyName = policyName;
        this.usageLimit = usageLimit;
        msg("Policy name/limit changed to " + policyName + "/" + usageLimit);

    }

    /**
     * @return the sessionStartUTC
     */
    public TimestampType getSessionStartUTC() {
        return sessionStartUTC;
    }

    /**
     * @return the remember
     */
    public boolean isRemember() {
        return remember;
    }

    /**
     * @param remember the remember to set
     */
    public void setRemember(boolean remember) {
        this.remember = remember;
    }

}
