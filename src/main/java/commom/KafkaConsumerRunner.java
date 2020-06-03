package commom;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import implementations.util.JCLConfigProperties;
import interfaces.kernel.JCL_result;

public class KafkaConsumerRunner extends Thread {
	/** 3.0 begin **/
	AtomicBoolean stop = new AtomicBoolean(false);
	KafkaConsumer<String, JCL_result> consumer;
	
	private static JCLResultResource localResourceGlobalVar;
	private static JCLResultResource localResourceExecute;
	private static Map<String, JCLResultResource> localResourceMapContainer;
	
	public KafkaConsumerRunner(JCLResultResource localResourceGlobalVarParam, JCLResultResource localResourceExecuteParam, Map<String, JCLResultResource> localResourceMapContainerParam) {
		localResourceGlobalVar = localResourceGlobalVarParam;
		localResourceExecute = localResourceExecuteParam;
		localResourceMapContainer = localResourceMapContainerParam;
	}

	@Override
	public void run() {
		Properties consumerProperties = JCLConfigProperties.get(Constants.Environment.JCLKafkaConfig());
		
		consumer =  new KafkaConsumer<>(
			consumerProperties,
			new StringDeserializer(),
			new JCLResultDeserializer()
		);
		
		try {
			while(!stop.get()) {
				consumer.subscribe(
					Pattern.compile(
						"^[a-zA-Z0-9]+$"
					)
				);
			
				ConsumerRecords<String, JCL_result> records = consumer.poll(Duration.ofNanos(Long.MAX_VALUE));
//				consumer.listTopics().forEach((k, v) -> {
//					System.out.println(k + ": " + v);
//				});
				
//				System.err.println("START CONSUMER THREAD");
				records.forEach(record -> {
//					System.out.println(record);
//					System.out.println("CONSUMED topic: " + record.topic() + ", key: " + record.key() + ", value: " + record.value().getCorrectResult());
					
					switch(record.key()) {
					case "ex":
						localResourceExecute.create(
							record.topic(),
							record.value()
						);
						break;
					case "gv":
						localResourceGlobalVar.create(
							record.topic(),
							record.value()
						);
						break;
					default:
						JCLResultResource aux = new JCLResultResource();
						
						if(localResourceMapContainer.containsKey(record.topic())) {
							aux = localResourceMapContainer.get(record.topic());
						}
						
						aux.create(record.key(), record.value());
						
						localResourceMapContainer.put(record.topic(), aux);
						
						break;
					}
				});
				consumer.commitAsync();
//				System.err.println("FINISH CONSUMER THREAD");
			}
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
