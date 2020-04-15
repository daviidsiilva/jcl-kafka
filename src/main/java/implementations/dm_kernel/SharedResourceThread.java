package implementations.dm_kernel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SharedResourceThread<T1, T2> extends Thread{
	
	protected final int port;	
	protected final Selector selector;
	protected final ServerSocketChannel serverSocket;
	/** 3.0 begin **/
	private Map<String, byte[]> localMemory;
	/** 3.0 end **/
	
	public SharedResourceThread(int port, Map<String, byte[]> localMemory) throws IOException{
		this.port = 4952;
		this.selector = Selector.open();
		this.serverSocket = ServerSocketChannel.open();
		
		/** 3.0 begin **/
		this.localMemory = localMemory;
		/** 3.0 end **/
	}
	
	public void end(){
		try {
			selector.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
				
	public void run(){
		openServerSocket();

		/** 3.0 begin **/		
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
				"localhost:9092");
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
			ByteArrayDeserializer.class.getName());
		
		Consumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		
		try {
			kafkaConsumer.subscribe(
				Arrays.asList("jcl-output")
			);
			
		} catch(Throwable t) {
			System
				.err
				.println(t);
			kafkaConsumer.close();
		}
		
		ConsumerRecords<String, byte[]> consumedRecords = kafkaConsumer.poll(Duration.ofNanos(Long.MAX_VALUE));
		
		try {
        	synchronized(this.localMemory) {
        		consumedRecords.forEach(
    				(record) -> {
    					if(record.value() != null) {
    						this.localMemory.put(record.key(), record.value());
    						
    						System.out.println("jcl-output = {"
								+ " partition:" + record.partition() 
								+ " offset:" + record.offset() 
								+ " key: " + record.key() 
								+ " value: " + record.value() + " }");    						
    					}
    					
    				}
				);
        	}
        	/** 3.0 end **/       	        
		} catch (Exception e) {
			e.printStackTrace();
       } finally {
    	   kafkaConsumer.close();
    	   
    	   try {
				selector.close();
				serverSocket.socket().close();
				serverSocket.close();
			} catch (Exception e) {
				// do nothing - server failed
			}
        }
	}	
	
    private void openServerSocket() {
        try {
        	this.serverSocket.configureBlocking(false);
            
        	//set some options
            this.serverSocket.socket().setReuseAddress(true);
            this.serverSocket.socket().bind(new InetSocketAddress(this.port));                                  
            this.serverSocket.register(this.selector,SelectionKey.OP_ACCEPT);

            
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port " + this.port, e);
        }
    }
}
