package net.misoitame.batch.listener;

import static spark.Spark.get;
import static spark.SparkBase.port;
import static spark.SparkBase.stop;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import net.misoitame.batch.sender.QueueSender;

/**
 * jms listener for sqs
 *
 */
public class ListenerDaemon {
	static CountDownLatch latch;

	public static void main(String[] args) {
		latch = new CountDownLatch(1);
		port(12345);

		get("/stop/", (req, res) -> {
			latch.countDown();
			return "accept stop signal.";
		});
		get("/queue/member/", (req, res) -> {
			String value = Optional.ofNullable(req.queryParams("value"))
					.orElse(String.valueOf(System.currentTimeMillis()));
			QueueSender.sendMemberQueue(value);
			return "member";
		});
		get("/queue/action/", (req, res) -> {
			String value = Optional.ofNullable(req.queryParams("value"))
					.orElse(String.valueOf(System.currentTimeMillis()));
			QueueSender.sendActionQueue(value);
			return "action";
		});
		// setup lister
		try {
			SQSConnectionFactory connectionFactory = SQSConnectionFactory.builder()
					.withRegion(Region.getRegion(Regions.AP_NORTHEAST_1)).build();
			SQSConnection connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			
			//member
			Queue queue = session.createQueue(QueueSender.QUEUE_MEMBER);
			MessageConsumer consumer = session.createConsumer(queue);
			consumer.setMessageListener(new MemberQueueListener());
			
			//action
			Queue aQueue = session.createQueue(QueueSender.QUEUE_ACTION);
			MessageConsumer aConsumer = session.createConsumer(aQueue);
			aConsumer.setMessageListener(new ActionQueueListener());
			
			connection.start();

		} catch (JMSException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Start receiving 
		try {
			latch.await();
			Thread.sleep(3000);
			stop();
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("over.");
	}

	private static class MemberQueueListener implements MessageListener {

		@Override
		public void onMessage(Message message) {
			try {
				if (message != null) {
					System.out.println("Member Received: " + ((TextMessage) message).getText());
				}
				message.acknowledge();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
	}

	private static class ActionQueueListener implements MessageListener {

		@Override
		public void onMessage(Message message) {
			try {
				if (message != null) {
					System.out.println("Action Received: " + ((TextMessage) message).getText());
				}
				message.acknowledge();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
	}
}
