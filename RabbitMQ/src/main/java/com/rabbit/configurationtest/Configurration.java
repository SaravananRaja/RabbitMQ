package com.rabbit.configurationtest;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.rabbitmq.client.ConnectionFactory;

public class Configurration {

	public static final String USERNAME = "guest";

	public static final String PASSWORD = "guest";

	public static final String HOSTNAME = "localhost";

	public static final int PORT = 15672;

	public static final String EMPTY_STRING = "";

	private String queueName;

	private String routingKey;

	private RabbitTemplate rabbitTemplate;

	public RabbitTemplate RabbitMQConfig(String queueName, String routingKey) {
		this.queueName = queueName;
		this.routingKey = routingKey;
		this.rabbitTemplate = getrabbitTemplate(queueName, routingKey);
		RabbitAdmin admin = new RabbitAdmin(this.rabbitTemplate.getConnectionFactory());
		admin.declareQueue(new Queue(this.queueName));
		return this.rabbitTemplate;
	}

	public RabbitTemplate getrabbitTemplate(String queueName, String routingKey) {

		RabbitTemplate template = new RabbitTemplate(connectionFactory());
//The routing key is set to the name of the queue by the broker for the default exchange.
		template.setRoutingKey(this.routingKey);
//Where we will synchronously receive messages from
		template.setQueue(this.queueName);
		// template.setMessageConverter(new JsonMessageConverter());
		return template;
	}

	public org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory() {
		ConnectionFactory connectionFactory = new ConnectionFactory();

		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		return (org.springframework.amqp.rabbit.connection.ConnectionFactory) connectionFactory;
	}

}
