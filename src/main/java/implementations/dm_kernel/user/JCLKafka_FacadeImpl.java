package implementations.dm_kernel.user;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import implementations.dm_kernel.ConnectorImpl;
import interfaces.kernel.JCL_connector;
import interfaces.kernel.JCL_message_register;
import interfaces.kernel.JCL_result;

public class JCLKafka_FacadeImpl extends JCL_FacadeImplLamb {
	@Override
	public Boolean register(String host,String port, String mac,String portS,JCL_message_register classReg){
		try {
			Properties properties = new Properties();
			
			properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			properties.put(ProducerConfig.CLIENT_ID_CONFIG, mac);
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
			
			Thread threadConsumer = new Thread() {
				public void run() {
					KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
				}	
			};
			threadConsumer.start();
//			return new KafkaProducer<>(properties);
//			return ((Boolean) result.getCorrectResult()).booleanValue();
			return false;

		} catch (Exception e) {

			System.err
					.println("problem in JCL facade register(File f, String classToBeExecuted)");
			e.printStackTrace();
			return false;
		}
	}
}
