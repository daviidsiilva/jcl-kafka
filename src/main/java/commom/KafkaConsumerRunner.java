package commom;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import interfaces.kernel.JCL_result;

public class KafkaConsumerRunner extends Thread {
	/** 3.0 begin **/
	AtomicBoolean stop = new AtomicBoolean(false);
	KafkaConsumer<String, JCL_result> consumer;
	
	private static JCLResultResource localResourceGlobalVar;
	private static JCLResultResource localResourceExecute;
	private static JCLResultResource localResourceMap;
	
	public KafkaConsumerRunner(JCLResultResource localResourceGlobalVarParam, JCLResultResource localResourceExecuteParam, JCLResultResource localResourceMapParam) {
		localResourceGlobalVar = localResourceGlobalVarParam;
		localResourceExecute = localResourceExecuteParam;
		localResourceMap = localResourceMapParam;
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
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
			"earliest");
		
		consumer =  new KafkaConsumer<>(
			consumerProperties,
			new StringDeserializer(),
			new JCLResultDeserializer()
		);
		
		try {
			consumer.subscribe(
				Pattern.compile(
					"^[a-zA-Z0-9]+"
				)
			);
			
			ConsumerRecords<String, JCL_result> records = consumer.poll(Duration.ofNanos(Long.MAX_VALUE));			
			
//			while(!stop.get()) {
			records.forEach(record -> {
				System.out.println(record);
//				System.out.println("topic: " + record.topic() + ", key: " + record.key() + ", value: " + record.value());
				if(record.key() == null) {
					
					
					localResourceExecute.create(
						record.topic(),
						record.value()
					);
				} else {
					switch(record.key()) {
					case "ex":
						localResourceExecute.create(
							record.topic(),
							record.value()
						);
						break;
					case "mp":
						localResourceMap.create(
							record.topic(),
							record.value()
						);
						break;
					default:
						localResourceGlobalVar.create(
							record.topic(),
							record.value()
						);
						break;
					}
				}
			});
//			}
		}catch (Exception e) {
			System.err.println("problem in KafkaConsumerRunner run()");
			
			e.printStackTrace();
		} 
		finally {
			consumer.close();
		}
	}
	
	public void shutdown() {
		stop.set(true);
	}
	/** 3.0 end **/
}
