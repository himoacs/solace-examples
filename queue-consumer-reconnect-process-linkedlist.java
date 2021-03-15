/**
 * Based on SimpleFlowToQueue.java example
 *
 * This sample demonstrates how to create a Flow to a durable Queue,
 * consume messages, and shove them into a LinkedList as soon as they are received.
 *
 * In a separate scheduled thread, there is logic to:
 *      - Iterate over all of the messages in the LinkedList
 *      - Based on some simulated failure, it will: 
 *            a.	either acknowledge the message and remove it from the LinkedList, or
 *            b.	‘close’ the FlowReceiver, and ‘start’ it again
 *            c.	Reiterate over the LinkedList till all the messages are acknowledged
 */

package com.solacesystems.jcsmp.samples.introsamples;

import java.util.Random;
import java.util.concurrent.*;
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.samples.introsamples.common.ArgParser;
import com.solacesystems.jcsmp.samples.introsamples.common.SampleApp;
import com.solacesystems.jcsmp.samples.introsamples.common.SessionConfiguration;
import com.solacesystems.jcsmp.samples.introsamples.common.SessionConfiguration.AuthenticationScheme;

import java.util.LinkedList;

public class SimpleFlowToQueue extends SampleApp implements XMLMessageListener, JCSMPStreamingPublishEventHandler {
    SessionConfiguration conf = null;

    // Create a LinkedList where we will store received messages
    public static LinkedList<BytesXMLMessage> linked_list_1 = new LinkedList<BytesXMLMessage>();

    // XMLMessageListener
    public void onException(JCSMPException exception) {
        exception.printStackTrace();
    }

    // XMLMessageListener
    public void onReceive(BytesXMLMessage message) {
        // Add received messages to our LinkedList to be processed separately
        linked_list_1.add(message);
    }

    // JCSMPStreamingPublishEventHandler
    public void handleError(String messageID, JCSMPException cause,
                            long timestamp) {
        cause.printStackTrace();
    }

    // JCSMPStreamingPublishEventHandler
    public void responseReceived(String messageID) {
    }

    void createSession(String[] args) throws InvalidPropertiesException {
        // Parse command-line arguments
        ArgParser parser = new ArgParser();
        if (parser.parse(args) == 0)
            conf = parser.getConfig();
        else
            printUsage(parser.isSecure());

        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, conf.getHost());
        properties.setProperty(JCSMPProperties.USERNAME, conf.getRouterUserVpn().get_user());
        if (conf.getRouterUserVpn().get_vpn() != null) {
            properties.setProperty(JCSMPProperties.VPN_NAME, conf.getRouterUserVpn().get_vpn());
        }
        properties.setProperty(JCSMPProperties.PASSWORD, conf.getRouterPassword());
        /*
         *  SUPPORTED_MESSAGE_ACK_CLIENT means that the received messages on the Flow
         *  must be explicitly acknowledged, otherwise the messages are redelivered to the client
         *  when the Flow reconnects.
         *  SUPPORTED_MESSAGE_ACK_CLIENT is used here to simply to show
         *  SUPPORTED_MESSAGE_ACK_CLIENT. Clients can use SUPPORTED_MESSAGE_ACK_AUTO
         *  instead to automatically acknowledge incoming Guaranteed messages.
         */
        properties.setProperty(JCSMPProperties.MESSAGE_ACK_MODE, JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

        // Disable certificate checking
        properties.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false);

        if (conf.getAuthenticationScheme().equals(AuthenticationScheme.BASIC)) {
            properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);
        } else if (conf.getAuthenticationScheme().equals(AuthenticationScheme.KERBEROS)) {
            properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_GSS_KRB);
        }

        // Channel properties
        JCSMPChannelProperties cp = (JCSMPChannelProperties) properties
                .getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        if (conf.isCompression()) {
            /*
             * Compression is set as a number from 0-9. 0 means
             * "disable compression" (the default) and 9 means max compression.
             * Selecting a non-zero compression level auto-selects the
             * compressed SMF port on the appliance, as long as no SMF port is
             * explicitly specified.
             */
            cp.setCompressionLevel(9);
        }
        session = JCSMPFactory.onlyInstance().createSession(properties);
    }

    void printUsage(boolean secure) {
        String strusage = ArgParser.getCommonUsage(secure);
        strusage += "This sample:\n";
        strusage += "\t[-d | --durable]\t Flow to a durable queue, default: temporary queue\n";
        System.out.println(strusage);
        finish(1);
    }

    public FlowReceiver getReceiver(String QUEUE_NAME) throws JCSMPException {
        // Create a Queue to receive messages.
        Queue queue;
        queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        // Create a receiver.
        FlowReceiver receiver = session.createFlow(queue, null, this);

        System.out.println("Subscribing to queue: " + receiver.getDestination());

        return receiver;
    }

    public void run(String[] args) {

        FlowReceiver receiver = null;

        class Task implements Runnable {
            private String name;
            LinkedList linked_list_1;
            FlowReceiver receiver;

            public Task(String name, LinkedList ll, FlowReceiver receiver) {
                this.name = name;
                this.linked_list_1 = ll;
                this.receiver = receiver;
            }

            public String getName() {
                return name;
            }

            public void run() {

                System.out.println("Executing tasks " + linked_list_1.size());

                try {
                    receiver = getReceiver("demo");
                    receiver.start();
                } catch (JCSMPException e) {
                    e.printStackTrace();
                }

                // Iterate over LinkedList which contains all the messages
                // Ack messages and remove them from LinkedList
                // Simulate failure and during failure, iterate over LinkedList from beginning
                // Till all messages are acked successfully
                while (linked_list_1.size()>0) {

                    for(int i = 0; i<linked_list_1.size(); i++) {

                        BytesXMLMessage message = (BytesXMLMessage) linked_list_1.get(i);
                        String msgID = message.getMessageId();

                        // Generate random boolean to simulate failure
                        Random rd = new Random();

                        System.out.println("Processing msgID: " + msgID);
                        if (rd.nextBoolean()) {
                            System.out.println("Acking msgID: " + msgID);
                            message.ackMessage();
                            // Remove acknowledged message
                            linked_list_1.remove(i);
                        } else {
                            System.out.println("Something went wrong. Not Acking msgID: " + msgID);
                            try {
                                System.out.println("Unbinding");
                                receiver.close();
                            } catch (NullPointerException e) {
                                e.printStackTrace();
                            }
                            try {
                                System.out.println("Rebinding");
                                receiver = getReceiver("demo");
                                receiver.start();
                                // Go back to beginning of the LinkedList
                                i = 0;
                            } catch (JCSMPException e) {
                                e.printStackTrace();
                            }
                        }
                        i++;
                    }
                }
            }
        }

        try {

            // Create the Session.
            createSession(args);

            // Open the data channel to the appliance.
            System.out.println("About to connect to appliance.");
            session.connect();
            System.out.println("Connected!");

            // Create a receiver and start it
            receiver = getReceiver("demo");
            receiver.start();

            Thread.sleep(1000);

            // Create a scheduled task executor which will execute our task responsible for
            // consuming messages from LinkedList and processing them
            ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
            Task task = new Task("Task", linked_list_1, receiver);
            System.out.println("Created : " + task.getName());

            executor.scheduleWithFixedDelay(task, 2, 5, TimeUnit.SECONDS);

            // Close the receiver.
            Thread.sleep(10000000);
            receiver.close();
            finish(0);
        } catch (JCSMPTransportException ex) {
            System.err.println("Encountered a JCSMPTransportException, closing receiver... " + ex.getMessage());
            if (receiver != null) {
                receiver.close();
                // At this point the receiver handle is unusable; a new one
                // should be created.
            }
            finish(1);
        } catch (JCSMPException ex) {
            System.err.println("Encountered a JCSMPException, closing receiver... " + ex.getMessage());
            // Possible causes:
            // - Authentication error: invalid username/password
            // - Invalid or unsupported properties specified
            if (receiver != null) {
                receiver.close();
                // At this point the receiver handle is unusable; a new one
                // should be created.
            }
            finish(1);
        } catch (Exception ex) {
            System.err.println("Encountered an Exception... " + ex.getMessage());
            finish(1);
        }
    }

    public static void main(String[] args) {
        SimpleFlowToQueue app = new SimpleFlowToQueue();
        app.run(args);
    }

}
