package commom;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import implementations.util.JCLConfigProperties;
import interfaces.kernel.JCL_result;

public class JCLKafkaConsumerThread extends Thread {
	
	AtomicBoolean stop = new AtomicBoolean(false);
	KafkaConsumer<String, JCL_result> consumer;
	
	private static JCLResultResource localResourceGlobalVar;
	private static JCLResultResource localResourceExecute;
	
	public JCLKafkaConsumerThread(JCLResultResource localResourceGlobalVarParam, JCLResultResource localResourceExecuteParam) {
		localResourceGlobalVar = localResourceGlobalVarParam;
		localResourceExecute = localResourceExecuteParam;
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
			consumer.subscribe(
				Pattern.compile(
					"^(?!MAP).*$"
				)
			);
			consumer.seekToBeginning(consumer.assignment());
			
			synchronized (this) {
				this.notify();
			}
					
			while(!stop.get()) {
				ConsumerRecords<String, JCL_result> records = consumer.poll(Duration.ofNanos(Long.MAX_VALUE));
				
				records.forEach(record -> {
//					System.out.print("");
//					System.out.println(record.key() + ":" + record);
//					System.out.println("record t:" + record.topic() + ", k:" + record.key() + ", v:" + record.value().getCorrectResult() + ", o:" + record.offset());
					
					switch(record.key()) {
					case Constants.Environment.EXECUTE_KEY:
						localResourceExecute.create(
							record.topic(),
							record.value()
						);
						break;
						
					case Constants.Environment.GLOBAL_VAR_KEY:
						localResourceGlobalVar.create(
							record.topic(),
							record.value()
						);
						break;
					
					case Constants.Environment.GLOBAL_VAR_LOCK_KEY:
						JCL_result value = new JCL_resultImpl();
						
						value.setCorrectResult(record.offset());
						
						localResourceGlobalVar.create(
							record.topic() + ":" + Constants.Environment.LOCK_PREFIX + ":" + record.value().getCorrectResult(),
							value
						);
						break;
					
					case Constants.Environment.GLOBAL_VAR_ACQUIRE:
						localResourceGlobalVar.create(
							record.topic() + ":" + Constants.Environment.GLOBAL_VAR_ACQUIRE,
							record.value()
						);
						break;
					
					case Constants.Environment.GLOBAL_VAR_RELEASE:
						try {
							JCL_result jclResultLockToken = localResourceGlobalVar.read(record.topic() + ":" + Constants.Environment.GLOBAL_VAR_ACQUIRE);
							
							localResourceGlobalVar.delete(
								record.topic() + ":" + Constants.Environment.LOCK_PREFIX + ":" + jclResultLockToken.getCorrectResult()
							);
							
							localResourceGlobalVar.delete(
								record.topic() + ":" + Constants.Environment.GLOBAL_VAR_ACQUIRE
							);
						} catch (Exception e1) {
							System.err
								.println("Problem in JCLKafkaConsumerThread case " + Constants.Environment.GLOBAL_VAR_RELEASE);
							e1.printStackTrace();
						}
						break;
					
					case Constants.Environment.GLOBAL_VAR_DEL:
						localResourceGlobalVar.delete(
							record.topic() 
						);
						break;
						
					default:
						break;
					}
				});
				
				consumer.commitAsync();
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
}
