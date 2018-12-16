/**
 * 
 */
package com.rabbitmq.producer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbit.configurationtest.Configurration;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Saravanan
 *
 */
public class Producer {

	/**
	 * Creating a New Queue without Exchange and send a Message.
	 *
	 * @param queueNAme : Mention the Queue Name.
	 * @param message   : Message Needs to be sent to a queue.
	 * @throws TimeoutException
	 */

	public void CreateQueueWithNewMessages(String queueName, Object message) throws TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(Configurration.HOSTNAME);
		Connection conn;
		try {
			conn = factory.newConnection();
			Channel channel = conn.createChannel();
			System.out.println("Queue Name is  " + queueName);
			System.out.println("Message is   " + message);
			channel.queueDeclare(queueName, false, false, false, null);
			channel.exchangeDeclare("testExchangework", "direct");
			channel.basicPublish("testExchangework", queueName, null, message.toString().getBytes("UTF-8"));
			channel.close();
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creating a New Queue Declare a queue
	 *
	 * Parameters: queue the name of the queue durable true if we are declaring a
	 * durable queue (the queue will survive a server restart) exclusive true if we
	 * are declaring an exclusive queue (restricted to this connection) autoDelete
	 * true if we are declaring an autodelete queue (server will delete it when no
	 * longer in use) arguments other properties (construction arguments) for the
	 * queue
	 *
	 */
	public boolean CreateQueue(String queue, boolean durable, boolean exclusive, boolean autoDelete,
			Map<String, Object> arguments) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		boolean created = false;
		factory.setHost(Configurration.HOSTNAME);
		Connection conn;
		try {
			conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
			created = true;
		} catch (IOException e) {
			System.out.println("Exception Occured " + e.getMessage());
			created = false;
		}
		return created;
	}

	
   /*
	*   Declare an exchange, via an interface that allows the complete set of arguments.
	*   Parameters:
	*   exchange the name of the exchange
	*	type the exchange type
	*	durable true if we are declaring a durable exchange (the exchange will survive a server restart)
	*	autoDelete true if the server should delete the exchange when it is no longer in use
	*	internal true if the exchange is internal, i.e. can't be directly published to by a client.
	*	arguments other properties (construction arguments) for the exchange
    *
	*/
	
	public boolean CreateExchange(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws Exception {
		
		ConnectionFactory factory = new ConnectionFactory();
		boolean created = false;
		factory.setHost(Configurration.HOSTNAME);
		Connection conn;
		try {
			conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
			created = true;
		} catch (IOException e) {
			System.out.println("Exception Occured " + e.getMessage());
			created = false;
		}
		return created;
	}

	/**
	 * Sending a new messages to New Exchange with Existing queue (TOPIC)
	 *
	 * @param queueNAme : Mention the Queue Name.
	 * @param message   : Message Needs to be sent to a queue.
	 * @throws Exception
	 */

	public void SendNewMessagesToNewExchangeWithRoutingKey(String pExchangeName, String pBuiltInExchangeTypes,
			String pRoutingKey, Object pMessages) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		if (pRoutingKey == null) {
			pRoutingKey = "";
		}
		try {
			factory.setHost(Configurration.HOSTNAME);
			if (pBuiltInExchangeTypes != null) {
				if (pBuiltInExchangeTypes.equalsIgnoreCase("direct")) {
					channel.exchangeDeclare(pExchangeName, BuiltinExchangeType.DIRECT);
					channel.basicPublish(pExchangeName, pRoutingKey, null, pMessages.toString().getBytes("UTF-8"));
					System.out.println(" [x] Sent '" + pMessages.toString() + "'");
				} else if (pBuiltInExchangeTypes.equalsIgnoreCase("fanout")) {
					channel.exchangeDeclare(pExchangeName, BuiltinExchangeType.FANOUT);
					channel.basicPublish(pExchangeName, pRoutingKey, null, pMessages.toString().getBytes("UTF-8"));
					System.out.println(" [x] Sent '" + pMessages.toString() + "'");
				} else if (pBuiltInExchangeTypes.equalsIgnoreCase("topic")) {
					channel.exchangeDeclare(pExchangeName, BuiltinExchangeType.TOPIC);
					channel.basicPublish(pExchangeName, pRoutingKey, null, pMessages.toString().getBytes("UTF-8"));
					System.out.println(" [x] Sent '" + pMessages.toString() + "'");
				} else if (pBuiltInExchangeTypes.equalsIgnoreCase("headers")) {
					channel.exchangeDeclare(pExchangeName, BuiltinExchangeType.HEADERS);
					channel.basicPublish(pExchangeName, pRoutingKey, null, pMessages.toString().getBytes("UTF-8"));
					System.out.println(" [x] Sent '" + pMessages.toString() + "'");
				}
			}
		} catch (Exception e) {
			System.out.println("Exception Occured " + e.getMessage());
		} finally {
			channel.close();
			connection.close();
		}

	}

	/**
	 * Sending a new messages to Existing Exchange with Existing queue
	 *
	 * @param queueNAme : Mention the Queue Name.
	 * @param message   : Message Needs to be sent to a queue.
	 * @throws Exception
	 */

	public void SendNewMessagesToExchangeWithRoutingKey(String pExchangeName, String pBuiltInExchangeTypes,
			String pRoutingKey, Object pMessages) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		try {
			factory.setHost(Configurration.HOSTNAME);
			channel.basicPublish(pExchangeName, pRoutingKey, null, pMessages.toString().getBytes("UTF-8"));
		} catch (Exception e) {
			System.out.println("Exception Occured " + e.getMessage());
		} finally {
			channel.close();
			connection.close();
		}
	}

}
