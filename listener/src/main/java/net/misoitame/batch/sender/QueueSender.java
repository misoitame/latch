package net.misoitame.batch.sender;

import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class QueueSender {

	public static String QUEUE_MEMBER = "q_member";
	public static String QUEUE_ACTION = "q_axtion";
	
	static AmazonSQSMessagingClientWrapper client;
	static SQSConnection connection;

	public static void sendMemberQueue(String value) throws Exception {
		sendQueue(QUEUE_MEMBER, value);
	}

	public static void sendActionQueue(String value) throws Exception {
		sendQueue(QUEUE_ACTION, value);
	}
	
	synchronized static void sendQueue(String name, String value) throws Exception {
		if (connection == null)
			init();

		client = connection.getWrappedAmazonSQSClient();
		// Create an SQS queue named 'TestQueue' – if it does not already exist.
		if (!client.queueExists(name)) {
			client.createQueue(name);
		}
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Queue queue = session.createQueue(name);

		MessageProducer producer = session.createProducer(queue);
		TextMessage message = session.createTextMessage(value);

		// Send the message.
		producer.send(message);
		session.close();
	}

	static void init() throws Exception {
		SQSConnectionFactory connectionFactory = SQSConnectionFactory.builder()
				.withRegion(Region.getRegion(Regions.AP_NORTHEAST_1)).build();
		connection = connectionFactory.createConnection();
	}
}
