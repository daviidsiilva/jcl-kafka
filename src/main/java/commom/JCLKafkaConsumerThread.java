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
	private static JCLResultResourceContainer localResourceMapContainer;
	
	public JCLKafkaConsumerThread(JCLResultResource localResourceGlobalVarParam, JCLResultResource localResourceExecuteParam, JCLResultResourceContainer localResourceMapContainerParam) {
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
			consumer.subscribe(
				Pattern.compile(
					"^[a-zA-Z0-9]+$"
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
					
					case Constants.Environment.MAP_PUT:
						JCLResultResource mapAux = null;
						
						try {
							if((localResourceMapContainer.isFinished()==false) || (localResourceMapContainer.getNumOfRegisters()!=0)){
								if ((mapAux = localResourceMapContainer.read(record.topic())) == null) {
									JCL_result jclResultInitHeader = new JCL_resultImpl();
									int size = 0;
									
									mapAux = new JCLResultResource();
									jclResultInitHeader.setCorrectResult(size);
									
									mapAux.create(Constants.Environment.MAP_HEADER_SIZE, jclResultInitHeader);
								}
							}
							
							JCL_result mapHeader = mapAux.read(Constants.Environment.MAP_HEADER_SIZE);
							int size = (int) mapHeader.getCorrectResult();
							size = size + 1;
							mapHeader.setCorrectResult(size);
							
							mapAux.create(Constants.Environment.MAP_HEADER_SIZE, mapHeader);
							
							ArrayList<Object> mapRecordPair = (ArrayList<Object>) record.value().getCorrectResult();
							JCL_result mapValue = new JCL_resultImpl();
							mapValue.setCorrectResult(mapRecordPair.get(1));
							mapAux.create(mapRecordPair.get(0).toString(), mapValue);
							
							localResourceMapContainer.create(record.topic(), mapAux);
						} catch (Exception e){
							System.err
								.println("problem in JCLKafkaConsumerThread");
							e.printStackTrace();
						}
						break;
						
					case Constants.Environment.MAP_INIT:
						JCLResultResource newMapResource = new JCLResultResource();
						JCL_result jclResultInitHeader = record.value();
						
						newMapResource.create(Constants.Environment.MAP_HEADER_SIZE, jclResultInitHeader);
						
						localResourceMapContainer.create(record.topic(), newMapResource);
						break;
					
					case Constants.Environment.MAP_LOCK:
						JCLResultResource mapLock = null;
						
						try {
							if((localResourceMapContainer.isFinished()==false) || (localResourceMapContainer.getNumOfRegisters()!=0)){
								while ((mapLock = localResourceMapContainer.read(record.topic())) == null);
							}
							
							JCL_result mapLockOffset = new JCL_resultImpl();
							mapLockOffset.setCorrectResult(record.offset());
							
							mapLock.create(
								record.topic() + ":" + Constants.Environment.LOCK_PREFIX + ":" + record.value().getCorrectResult(), 
								mapLockOffset
							);
							
							localResourceMapContainer.create(record.topic(), mapLock);
						} catch (Exception e) {
							System.err
								.println("problem in JCLKafkaConsumerThread case: " + Constants.Environment.MAP_LOCK);
						}
						break;
						
					case Constants.Environment.MAP_ACQUIRE:
						JCLResultResource mapAcquire = null;
						
						try {
							if((localResourceMapContainer.isFinished()==false) || (localResourceMapContainer.getNumOfRegisters()!=0)){
								while ((mapAcquire = localResourceMapContainer.read(record.topic())) == null);
							}
							
							mapAcquire.create(
								record.topic() + ":" + Constants.Environment.MAP_ACQUIRE, 
								record.value()
							);
							
							localResourceMapContainer.create(record.topic(), mapAcquire);
						} catch (Exception e) {
							System.err
								.println("problem in JCLKafkaConsumerThread case: " + Constants.Environment.MAP_ACQUIRE);
						}
						break;
					
					case Constants.Environment.MAP_RELEASE:
						JCLResultResource mapRelease = null;
						
						try {
							if((localResourceMapContainer.isFinished()==false) || (localResourceMapContainer.getNumOfRegisters()!=0)){
								while ((mapRelease = localResourceMapContainer.read(record.topic())) == null);
							}
							
							JCL_result jclResultLockToken = mapRelease.read(record.topic() + ":" + Constants.Environment.MAP_ACQUIRE);
							
							mapRelease.delete(
								record.topic() + ":" + Constants.Environment.LOCK_PREFIX + ":" + jclResultLockToken.getCorrectResult()
							);
							
							mapRelease.delete(
								record.topic() + ":" + Constants.Environment.MAP_ACQUIRE
							);
						} catch (Exception e1) {
							System.err
								.println("Problem in JCLKafkaConsumerThread case " + Constants.Environment.MAP_RELEASE);
							e1.printStackTrace();
						}
						break;
						
					default:
						JCLResultResource aux = null;
						
						try {
							if((localResourceMapContainer.isFinished()==false) || (localResourceMapContainer.getNumOfRegisters()!=0)){
								if ((aux = localResourceMapContainer.read(record.topic())) == null) {
									aux = new JCLResultResource();
								}
							}
							
							aux.create(record.key(), record.value());
							localResourceMapContainer.create(record.topic(), aux);
						} catch (Exception e){
							System.err
								.println("problem in JCLKafkaConsumerThread");
							e.printStackTrace();
						}
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
