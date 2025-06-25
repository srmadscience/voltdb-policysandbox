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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Runnable class to read Kafka messages and log them.
 *
 */
public class ConsoleMessageConsumer implements Runnable {

    /**
     * Comma delimited list of Kafka hosts. Note we expect the port number with 
     * each host name
     */
    String hostnames;
    
    /**
     * Keep running until told to stop..
     */
    boolean keepGoing = true;
    
    /**
     * Used for formatting messages
     */
    static SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Create a runnable instance of a class to poll the Kafka topic console_messages
     * @param hostnames - hostname1:9092,hostname2:9092 etc
     */
    public ConsoleMessageConsumer(String hostnames) {
        super();
        this.hostnames = hostnames;
    }

    @Override
    public void run() {

        try {

            Properties props = new Properties();
            props.put("bootstrap.servers", hostnames);
            props.put("group.id", "PolicyChangeConsoleMessageConsumer");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.commit.interval.ms", "100");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("console_messages"));

            while (keepGoing) {

                @SuppressWarnings("deprecation")
                ConsumerRecords<String, String> records = consumer.poll(100); //TODO
                for (ConsumerRecord<String, String> record : records) {

                    ConsoleMessageConsumer.msg(record.value());

                }
            }

            consumer.close();

        } catch (Exception e1) {
            ConsoleMessageConsumer.msg(e1);
        }

    }

    /**
     * Stop polling for messages and exit.
     */
    public void stop() {
        keepGoing = false;

    }
    
    /**
     * Print a formatted message.
     * 
     * @param message
     */
    public static void msg(String message) {

        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Print a formatted message.
     * 
     * @param e
     */
    public static void msg(Exception e) {

        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + e.getClass().getName() + ":" + e.getMessage());

    }


}
