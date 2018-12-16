package com.rabbitmq.producer;

public class MainClass {

	public static void main(String[] args) throws Exception {

		Producer producer = new Producer();
	//	producer.checkRabbitMQAMQPProtocol("This is message numero " );
		// producing some messages
		for (int i = 1; i < 6; i++) {
			final String message = "This is message numero with values" + i;

			
			//producer.CreateQueueWithNewMessages("testQueueab", message);
			producer.SendNewMessagesToNewExchangeWithRoutingKey("TESTEXCHANGETWONEW", "Direct","test", message);
		}
	}

}
