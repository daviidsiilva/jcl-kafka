package commom;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;


public class KafkaConsumerRunner extends Thread {
	/** 3.0 begin **/
	private Map<Object, Object> localMemory;
	
	public KafkaConsumerRunner(Map<Object, Object> localMemory) {
		this.localMemory = localMemory;
	}

	@Override
	public void run() {
		Vertx vertx = Vertx.vertx();
		Properties consumerProperties = new Properties();

		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
			"localhost:9092");
		consumerProperties.put(
			ConsumerConfig.CLIENT_ID_CONFIG, 
			"jcl-client");
		consumerProperties.put(
			ConsumerConfig.GROUP_ID_CONFIG, 
			"jcl-consumer-group");
		consumerProperties.put(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
			StringDeserializer.class.getName());
		consumerProperties.put(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
			ByteArrayDeserializer.class.getName());
		consumerProperties.put(
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
			"earliest");
		
		KafkaConsumer<String, String> consumer = KafkaConsumer.create(
			vertx, 
			consumerProperties
		);
		
		try {
			consumer.subscribe(Pattern.compile("."));
			
			consumer.handler(record -> {
				System.out.println(record);
				
				synchronized(this) {
					this.localMemory.put(
							record.topic(), 
							record.value()
							);
					
					this.notifyAll();
				}
			});
		} finally {
			consumer.close();
		}
	}
	/** 3.0 end **/
}
