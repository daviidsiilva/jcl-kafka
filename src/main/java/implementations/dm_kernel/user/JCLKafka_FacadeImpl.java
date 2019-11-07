package implementations.dm_kernel.user;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import implementations.dm_kernel.ConnectorImpl;
import implementations.dm_kernel.MessageGlobalVarObjImpl;
import implementations.util.ObjectWrap;
import interfaces.kernel.JCL_connector;
import interfaces.kernel.JCL_message_global_var_obj;
import interfaces.kernel.JCL_message_register;
import interfaces.kernel.JCL_result;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

import implementations.util.KafkaProperties;

public class JCLKafka_FacadeImpl extends JCL_FacadeImplLamb {
	
	@Override
	public Boolean register(String host,String port, String mac,String portS,JCL_message_register classReg){

		try {
			Properties kafkaProperties = new Properties();
				
			kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-jcl");
			kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
			kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
				
			final StreamsBuilder builder = new StreamsBuilder();
				
			KStream<String, String> source = builder.stream("streams-jcl-input");

			source.to("streams-jcl");

			return true;
		} catch (Exception e) {

			System.err
					.println("problem in JCL facade register(File f, String classToBeExecuted)");
			e.printStackTrace();
			return false;
		}
	}

	public Object[] execute(
		String objectNickname,
		String host,
		String port, 
		String mac, 
		String portS,
		boolean hostChange, 
		Object... args
	) {
		try {		
			
			Properties properties = new KafkaProperties().get(host, port, objectNickname);
			
			Thread threadProducer = new Thread() {
				public void run() {
					KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
					
					try {
						final ProducerRecord<String, String> record = new ProducerRecord<>(
							objectNickname, 
							objectNickname,
							objectNickname
						);

						RecordMetadata metadata = kafkaProducer.send(record).get();
						
						System.out.println(metadata);
					} catch(Throwable t) {
						kafkaProducer.flush();
						kafkaProducer.close();

						System
							.err
							.println(t);
					}
				}	
			};
			
			Thread threadConsumer = new Thread() {
				public void run() {
					KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
					
					try {
						kafkaConsumer.subscribe(Arrays.asList(objectNickname));
					} catch(Throwable t) {
						kafkaConsumer.close();
					}
				}
			};
			
			try {
				threadProducer.start();
				threadConsumer.start();
				
				return new Object[]{
					host,
					port,
					mac,
					portS
				};
			
			} catch(Throwable t) {
				System
					.err
					.println(t);
				System.exit(1);
			}

		} catch (Exception e) {
			System.err
					.println("JCL facade problem in execute(String className, Object... args)");
					e.printStackTrace();
			return null;
		}

		return new Object[]{
			host,
			port,
			mac,
			portS
		};
	}
	
	public boolean instantiateGlobalVar(
			Object key,
			String nickName, 
			Object[] defaultVarValue,
			String host,
			String port, 
			String mac, 
			String portS, 
			int hostId) {
		try {
			
			Properties properties = new KafkaProperties().get(host, port, nickName);
			
				// ################ Serialization key ########################
				LinkedBuffer buffer = LinkedBuffer.allocate(1048576);
				ObjectWrap objW = new ObjectWrap(key);					
//				byte[] byK = ProtobufIOUtil.toByteArray(objW,scow, buffer);			
				// ################ Serialization key ########################

//				JCL_message_global_var_obj gvMessage = new MessageGlobalVarObjImpl(nickName, byK, defaultVarValue);
//				gvMessage.setType(9);
				
				JCL_connector globalVarConnector = new ConnectorImpl();
				globalVarConnector.connect(host, Integer.parseInt(port),mac);
//				JCL_result result = globalVarConnector.sendReceive(gvMessage,portS).getResult();
				globalVarConnector.disconnect();
				
				// result from host
//				return (Boolean) result.getCorrectResult();
				return false;
		} catch (Exception e) {
			System.err
					.println("problem in JCL facade instantiateGlobalVar(String nickName, String varName, File[] jars, Object[] defaultVarValue)");
			return false;
		}
	}
}
