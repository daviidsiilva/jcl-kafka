package commom;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerRunner extends Thread {
	/** 3.0 begin **/
	AtomicBoolean stop = new AtomicBoolean(false);
	KafkaConsumer<String, String> consumer;
	
	private Map<Object, Object> localMemory;
	
	public KafkaConsumerRunner(Map<Object, Object> localMemory) {
		this.localMemory = localMemory;
	}

	@Override
	public void run() {
		Properties consumerProperties = new Properties();

		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
			"localhost" + ":" + "9092");
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
			StringDeserializer.class.getName());
		consumerProperties.put(
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
			"earliest");
		
		consumer =  new KafkaConsumer<>(
			consumerProperties
		);
		
		try {
			consumer.subscribe(
				Pattern.compile(
					"^[a-zA-Z0-9]+$"
				)
			);
			
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofNanos(Long.MAX_VALUE));			
			
//			while(!stop.get()) {
				records.forEach(record -> {
					System.out.println(record);
					synchronized(this) {
						this.localMemory.put(
							record.topic(), 
							record.value()
						);
						
						this.notify();
						System.out.println("notified!");
						System.out.println(this);
					}
				});
//			}
		} finally {
			consumer.close();
		}
	}
	
	public void shutdown() {
		stop.set(true);
		
		consumer.wakeup();
	}
	/** 3.0 end **/
}
