package implementations.dm_kernel.host;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

//Kafka imports
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaHost extends MainHost{

	public KafkaHost(int port, String BoardID) throws IOException {
		super(port, BoardID);
		// TODO Auto-generated constructor stub
	}
	
	private Properties readPropertiesFile() {
		Properties properties = new Properties();
		try {
		    properties.load(new FileInputStream("../jcl_conf/config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}

	@Override
	protected void beforeListening() {
		
		Properties properties = this.readPropertiesFile();
		
		serverAdd = properties.getProperty("serverMainAdd");
		serverPort = Integer.parseInt(properties.getProperty("superPeerMainPort"));

		Thread kafkaRegister = new Thread() {
			public void run() {
				Properties kafkaProperties = new Properties();
				
				kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-jcl");
				kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
				kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
				
				final StreamsBuilder builder = new StreamsBuilder();
				
				KStream<String, String> source = builder.stream("streams-jcl-input");
				
				source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
					.groupBy((key, value) -> value)
	                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("store"))
	                .toStream()
	                .to("streams-jcl-output", Produced.with(Serdes.String(), Serdes.Long()));

				final Topology topology = builder.build();

		        final KafkaStreams streams = new KafkaStreams(topology, properties);

		        final CountDownLatch latch = new CountDownLatch(1);

		        System.out.println(topology.describe());

		        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
		            @Override
		            public void run () {
		                streams.close();
		                latch.countDown();
		            }
		        });
		        
		        try {
		            streams.start();
		            latch.await();
		        } catch (Throwable t) {
		            System.exit(1);
		        }
			}
		};
		
		try {
			kafkaRegister.start();
			System
				.out
				.println("HOST JCL/Kafka is OK");
		} catch(Throwable t) {
			System
				.err
				.println("HOST JCL/Kafka NOT STARTED");
			System
				.err
				.println(t);
			System.exit(1);
		}
	}
}
