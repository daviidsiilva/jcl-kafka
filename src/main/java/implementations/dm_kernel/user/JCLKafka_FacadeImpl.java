package implementations.dm_kernel.user;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import interfaces.kernel.JCL_message_register;

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
			
			Properties properties = new Properties();
			
			properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
			properties.put(ProducerConfig.CLIENT_ID_CONFIG, objectNickname);
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			
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

					} catch(Throwable t) {
						kafkaProducer.flush();
						kafkaProducer.close();

						System
							.err
							.println(t);
					}
				}	
			};
			
			try {
				threadProducer.start();
				
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
}
