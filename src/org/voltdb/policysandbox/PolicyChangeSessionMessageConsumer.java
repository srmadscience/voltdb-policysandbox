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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.BasicConfigurator;

/**
 * Runnable class to receive policy change messages and update our collection of
 * virtual sessions.
 *
 */
public class PolicyChangeSessionMessageConsumer implements Runnable {

    /**
     * Handle for data gebnerator so we can update its sessions
     */
    PolicyDataGenerator pdg = null;

    /**
     * Comma delimited list of Kafka hosts. Note we expect the port number with each
     * host name
     */
    String hostnames;

    /**
     * Keep running until told to stop..
     */
    boolean keepGoing = true;

    /**
     * Create a runnable instance of a class to poll the Kafka topic
     * policy_change_session_messages
     * 
     * @param pdg       - An instance of our policy session emulator
     * @param hostnames - hostname1:9092,hostname2:9092 etc
     */
    public PolicyChangeSessionMessageConsumer(PolicyDataGenerator pdg, String hostnames) {
        super();
        this.pdg = pdg;
        this.hostnames = hostnames;
    }

    @Override
    public void run() {

        try {

            Properties props = new Properties();
            props.put("bootstrap.servers", hostnames);
            props.put("group.id", "PolicyChangeSessionMessageConsumer");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.commit.interval.ms", "100");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("policy_change_session_messages"));
            long messageCounter = 0;

            while (keepGoing) {

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    if (messageCounter++ % 1000000 == 999999) {
                        ConsoleMessageConsumer.msg("Received " + messageCounter + " policy change messages via Kafka");
                    }

                    PolicyChangeMessage newMessage = new PolicyChangeMessage(record.key(), record.value());

                    pdg.reportPolicyChange(newMessage);

                }
            }

            consumer.close();

        } catch (Exception e1) {
            ConsoleMessageConsumer.msg(e1.getMessage());
        }

    }

    /**
     * Stop polling for messages and exit.
     */
    public void stop() {
        keepGoing = false;
    }

}
