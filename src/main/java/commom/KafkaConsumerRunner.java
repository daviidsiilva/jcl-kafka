package commom;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerRunner implements Runnable {

	private KafkaConsumer<Object, Object> consumer;
	
	private Map<Object, Object> localMemory;
	private Object key;

	public KafkaConsumerRunner(Map<Object, Object> localMemory, Object key) {
		this.localMemory = localMemory;
		this.key = key;
	}

	@Override
	public void run() {
		/** 3.0 begin **/
		Properties consumerProperties = new Properties();

		consumerProperties.put(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
			"localhost:9092"
		);
		consumerProperties.put(
			ConsumerConfig.GROUP_ID_CONFIG, 
			"jcl-consumer-group"
		);
		consumerProperties.put(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
			StringDeserializer.class.getName()
		);
		consumerProperties.put(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
			StringDeserializer.class.getName()
		);

		this.consumer = new KafkaConsumer<>(consumerProperties);

		try {
			consumer.subscribe(
				Arrays.asList(
					key.toString()
				)
			);

			ConsumerRecords<Object, Object> consumedRecords = this.consumer
				.poll(Duration.ofNanos(Long.MAX_VALUE));

			synchronized(this.localMemory) {
				consumedRecords.forEach(
					(record) -> {
						
						this.localMemory.put(
							record.topic(), 
							record.value()
						);
					}
				);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		/** 3.0 end **/
	}

	public void shutdown() {
		consumer.wakeup();
		consumer.close();
	}
}
