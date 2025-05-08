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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import org.voltdb.client.topics.VoltDBKafkaPartitioner;
import org.voltdb.types.TimestampType;

/**
 * 
 *
 */
public class PolicyDataGenerator {

    /**
     * We track a known executive user when running this demo
     */
    private static final Long KNOWN_EXECUTIVE_SESSION_ID = 0l;

    /**
     * We track a known average user when running this demo
     */
    private static final Long KNOWN_AVERAGE_SESSION_ID = 20000l;

    /**
     * We track a known student user when running this demo
     */
    private static final Long KNOWN_STUDENT_SESSION_ID = 100001l;

    /**
     * Our handle to VoltDB
     */
    Client voltClient = null;

    /**
     * Comma delimited list of hosts *without* port numbers.
     */
    String hostnames;

    /**
     * How many sessions / users to create. You can create more than 1 session per
     * user by running multiple instances and using the 'offset' parameter
     */
    int userCount;

    /**
     * Target transactions per millisecond
     */
    int tpMs;

    /**
     * How many seconds to run for.
     */
    int durationSeconds;

    /**
     * How many network 'cells' to have
     */
    int cellCount;

    /**
     * An arbitrary number to add to userId to get sessionId. Used when you want to run
     * more than 1 copy at once.
     */
    int offset;
    
    /**
     * HashMap containing our sessions. It will get to be big...
     */
    HashMap<Long, PolicySession> sessionMap = new HashMap<Long, PolicySession>(userCount);

    /**
     * Shared Random instance.
     */
    Random r = new Random();

    /**
     * UTC time we started running
     */
    long startMs;

    /**
     * 
     * Class to simulate millions of policy sessions as part of the policy sandbox
     * 
     * @param hostnames
     * @param userCount
     * @param tpMs
     * @param durationSeconds
     * @param cellCount
     * @param offset
     * @throws Exception
     */
    public PolicyDataGenerator(String hostnames, int userCount, int tpMs, int durationSeconds, int cellCount
            , int offset)
            throws Exception {

        this.hostnames = hostnames;
        this.userCount = userCount;
        this.tpMs = tpMs;
        this.durationSeconds = durationSeconds;
        this.cellCount = cellCount;
        this.offset = offset;

        ConsoleMessageConsumer.msg("hostnames=" + hostnames + ", users=" + userCount + ", tpMs=" + tpMs
                + ",durationSeconds=" + durationSeconds + ", cellCount=" + cellCount
                + ", offset=" + offset);

        ConsoleMessageConsumer.msg("Log into VoltDB");
        voltClient = connectVoltDB(hostnames);

    }

    /**
     * Run our simulation...
     */
    public void run() {

        long laststatstime = System.currentTimeMillis();
        startMs = System.currentTimeMillis();

        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;
        long recordCount = 0;
        long lastReportedRecordCount = 0;
        
        // For illustrative purposes we track a sessionm for each of our policies...
        PolicySession averageSession = null;
        PolicySession executiveSession = null;
        PolicySession studentSession = null;

        ConsoleMessageConsumer.msg("Run started");

        synchronized (sessionMap) {

            executiveSession = new PolicySession(new TimestampType(), KNOWN_EXECUTIVE_SESSION_ID + offset,
                    KNOWN_EXECUTIVE_SESSION_ID, 0, r);
            executiveSession.setRemember(true);
            sessionMap.put(KNOWN_EXECUTIVE_SESSION_ID + offset, executiveSession);
            sendNewSessionMessage(executiveSession);

            averageSession = new PolicySession(new TimestampType(), KNOWN_AVERAGE_SESSION_ID + offset,
                    KNOWN_AVERAGE_SESSION_ID, 0, r);
            averageSession.setRemember(true);
            sessionMap.put(KNOWN_AVERAGE_SESSION_ID + offset, averageSession);
            sendNewSessionMessage(averageSession);

            studentSession = new PolicySession(new TimestampType(), KNOWN_STUDENT_SESSION_ID + offset,
                    KNOWN_STUDENT_SESSION_ID, 0, r);
            studentSession.setRemember(true);
            sessionMap.put(KNOWN_STUDENT_SESSION_ID + offset, studentSession);
            sendNewSessionMessage(studentSession);
        }

        while (System.currentTimeMillis() < (startMs + (1000 * durationSeconds))) {

            recordCount++;

            // pick a random session and cell id..
            long randomSessionId = r.nextInt(userCount) + offset;
            int randomCellId = r.nextInt(cellCount);

            synchronized (sessionMap) {

                // See if our session already exists. If it does then generate some usage. If not, 
                // create it...
                PolicySession ourSession = sessionMap.get(randomSessionId);
                
                if (ourSession == null) {
                    ourSession = new PolicySession(new TimestampType(), randomSessionId, r.nextInt(userCount),
                            randomCellId, r);
                    sessionMap.put(randomSessionId, ourSession);

                    sendNewSessionMessage(ourSession);

                } else {
                    
                    synchronized (ourSession) {

                        PolicyUsageMessage newMessage = ourSession.getNextUsageMessage();

                        sendSessionUsageMessage(newMessage);

                    }
                }

            }

            if (tpThisMs++ > tpMs) {

                // but sleep if we're moving too fast...
                while (currentMs == System.currentTimeMillis()) {
                    try {
                        Thread.sleep(0, 50000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            // Every 10 seconds dump stats to console...
            if (laststatstime + 10000 < System.currentTimeMillis()) {

                double recordsProcessed = recordCount - lastReportedRecordCount;
                double tps = 1000 * (recordsProcessed / (System.currentTimeMillis() - laststatstime));

                ConsoleMessageConsumer.msg("Offset = " + offset + " Record " + recordCount + " TPS=" + (long) tps);
                ConsoleMessageConsumer.msg("Active Sessions: " + sessionMap.size());

                laststatstime = System.currentTimeMillis();
                lastReportedRecordCount = recordCount;

                printApplicationStats(voltClient, executiveSession, averageSession, studentSession);
            }

        }

        ConsoleMessageConsumer.msg("Run finished; ending sessions");

        laststatstime = System.currentTimeMillis();

        // End sessions
        synchronized (sessionMap) {

            for (Map.Entry<Long, PolicySession> entry : sessionMap.entrySet()) {

                PolicySession endingSession = entry.getValue();

                sendEndSessionMessage(endingSession);

                if (tpThisMs++ > tpMs) {

                    // but sleep if we're moving too fast...
                    while (currentMs == System.currentTimeMillis()) {
                        try {
                            Thread.sleep(0, 50000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }

                if (laststatstime + 10000 < System.currentTimeMillis()) {

                    double recordsProcessed = recordCount - lastReportedRecordCount;
                    double tps = 1000 * (recordsProcessed / (System.currentTimeMillis() - laststatstime));

                    ConsoleMessageConsumer.msg("Offset = " + offset + " Record " + recordCount + " TPS=" + (long) tps);

                    laststatstime = System.currentTimeMillis();
                    lastReportedRecordCount = recordCount;

                    printApplicationStats(voltClient, executiveSession, averageSession, studentSession);
                }

            }

        }

        try {
            voltClient.drain();
        } catch (Exception e) {
            ConsoleMessageConsumer.msg(e);
        }

        ConsoleMessageConsumer.msg("done...");

    }

    /**
     * Send New Session message directly to VoltDB
     * 
     * @param newSession
     */
    private void sendNewSessionMessage(PolicySession newSession) {

        if (voltClient != null) {
            try {
                RememberPolicyCreationDetailsCallback rpdc = new RememberPolicyCreationDetailsCallback(newSession);
                voltClient.callProcedure(rpdc, "ReportNewSession", newSession.getParamsForVoltDBCall());
            } catch (Exception e) {
                ConsoleMessageConsumer.msg(e.getMessage());
            }
        }

    }

    /**
     * Send Usage Message directly to VoltDB
     * 
     * @param usageMessage
     */
    private void sendSessionUsageMessage(PolicyUsageMessage usageMessage) {

        if (voltClient != null) {
            try {
                ComplainOnErrorCallback coec = new ComplainOnErrorCallback();
                voltClient.callProcedure(coec, "ReportSessionUsage", usageMessage.getParamsForVoltDBCall());
            } catch (Exception e) {
                ConsoleMessageConsumer.msg(e.getMessage());
            }
        }

    }

    /**
     * Send End Session message directly to VoltDB
     * 
     * @param endingSession
     */
    private void sendEndSessionMessage(PolicySession endingSession) {

        if (voltClient != null) {
            try {
                ComplainOnErrorCallback coec = new ComplainOnErrorCallback();
                voltClient.callProcedure(coec, "ReportEndSession", endingSession.getParamsForVoltDBCall());
            } catch (Exception e) {
                ConsoleMessageConsumer.msg(e.getMessage());
            }
        }

    }

    /**
     * Update session hashmap with new policy information. This is called from
     * PolicyChangeSessionMessageConsumer.
     * 
     * @param policyChangeMessage
     */
    public void reportPolicyChange(PolicyChangeMessage policyChangeMessage) {
        synchronized (sessionMap) {

            PolicySession changedSession = sessionMap.get(policyChangeMessage.getSessionId());

            if (changedSession != null && policyChangeMessage.getChangeTimestamp().asExactJavaDate().getTime() > startMs
                    && policyChangeMessage.getSessionStartUTC().asExactJavaDate().getTime() == changedSession
                            .getSessionStartUTC().asExactJavaDate().getTime()) {
                changedSession.changePolicy(policyChangeMessage);
            }
        }

    }

    /**
     * Run our Policy Demo. This actually involves three activities - emulating a large 
     * number of sessions, using Kafka to absorb policy changes and using Kafka to print 
     * console messages about activity inside VoltDB.
     * 
     * @param args PolicyDataGenerator hostnames userCount tpMs durationSeconds cellCount offset
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 6) {
            ConsoleMessageConsumer
                    .msg("Usage: PolicyDataGenerator hostnames userCount tpMs durationSeconds cellCount offset");
            System.exit(1);
        }

        String hostnames = args[0];
        int userCount = Integer.parseInt(args[1]);
        int tpMs = Integer.parseInt(args[2]);
        int durationSeconds = Integer.parseInt(args[3]);
        int cellCount = Integer.parseInt(args[4]);

        int offset = Integer.parseInt(args[5]);
        PolicyDataGenerator pdg = new PolicyDataGenerator(hostnames, userCount, tpMs, durationSeconds, cellCount,offset);

        PolicyChangeSessionMessageConsumer pcmc = new PolicyChangeSessionMessageConsumer(pdg, addPort(hostnames, 9092));
        ConsoleMessageConsumer cccmc = new ConsoleMessageConsumer(addPort(hostnames, 9092));
        
        Thread pcmcRunner = new Thread(pcmc);
        pcmcRunner.start();

        Thread cccmcRunner = new Thread(cccmc);
        cccmcRunner.start();

        pdg.run();

        pcmc.stop();
        cccmc.stop();

    }

    /**
     * Add port 'port' to each hostname in comma delimited list
     * @param hostnames
     * @param port
     * @return comma delimited list of hostnames plus ports
     */
    private static String addPort(String hostnames, int port) {

        StringBuffer b = new StringBuffer();
        String[] hostnamesArray = hostnames.split(",");
        String comma = "";

        for (int i = 0; i < hostnamesArray.length; i++) {
            b.append(comma);
            comma = ",";

            b.append(hostnamesArray[i]);
            b.append(':');
            b.append(port);
        }

        return b.toString();
    }

    /**
     * 
     * Connect to VoltDB using native APIS
     * 
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            ConsoleMessageConsumer.msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            //config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (int i = 0; i < hostnameArray.length; i++) {
                ConsoleMessageConsumer.msg("Connect to " + hostnameArray[i] + "...");
                try {
                    client.createConnection(hostnameArray[i]);
                } catch (Exception e) {
                    ConsoleMessageConsumer.msg(e.getMessage());
                }
            }

            if (client.getConnectedHostList().size() == 0) {
              throw new Exception("No hosts usable...");  
            }
            
            ConsoleMessageConsumer.msg("Connected to VoltDB");

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    /**
     * Check VoltDB to see how things are going...
     * 
     * @param client
     * @param executiveSession
     * @param averageSession
     * @param studentSession
     */
    public static void printApplicationStats(Client client, PolicySession executiveSession,
            PolicySession averageSession, PolicySession studentSession) {

        ConsoleMessageConsumer.msg("");
        ConsoleMessageConsumer.msg("Latest Stats:");
        ConsoleMessageConsumer.msg("");

        try {
            ClientResponse cr = client.callProcedure("ShowPolicyStatus__promBL");
            if (cr.getStatus() == ClientResponse.SUCCESS) {
                VoltTable[] resultsTables = cr.getResults();
                for (int i = 0; i < resultsTables.length; i++) {
                    if (resultsTables[i].advanceRow()) {
                        ConsoleMessageConsumer.msg(resultsTables[i].toFormattedString());
                    }

                }

            }
        } catch (IOException | ProcCallException e) {

            e.printStackTrace();
        }

        if (executiveSession != null) {
            ConsoleMessageConsumer.msg("Executive:" + executiveSession.toString());
        }

        if (averageSession != null) {
            ConsoleMessageConsumer.msg("Average:" + averageSession.toString());
        }

        if (studentSession != null) {
            ConsoleMessageConsumer.msg("Student:" + studentSession.toString());
        }

    }

}
