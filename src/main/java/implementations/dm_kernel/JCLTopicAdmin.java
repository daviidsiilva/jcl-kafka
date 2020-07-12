package implementations.dm_kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import commom.Constants;
import implementations.util.KafkaConfigProperties;

public class JCLTopicAdmin {
	private static JCLTopicAdmin instance;
	private static Properties topicProperties;
	private static AdminClient adminClient;
	private static Set<String> serverTopicsSet;
			
	private JCLTopicAdmin() {
		serverTopicsSet = ConcurrentHashMap.newKeySet();
		topicProperties = KafkaConfigProperties.getInstance().get();
		adminClient = AdminClient.create(topicProperties);
	}
	
	public static JCLTopicAdmin getInstance() {
		if(instance != null) {
			return instance;
		}
		
		return new JCLTopicAdmin();
	}
	
	public void create(final Properties properties) {
		try {
			final List<NewTopic> topics = new ArrayList<>();

			topics.add(
				new NewTopic(
					properties.getProperty("topic.name"),
					Integer.parseInt(properties.getProperty("topic.partitions")),
					Short.parseShort(properties.getProperty("topic.replication.factor"))
				)
			);

			adminClient.createTopics(topics);
			
			final CreateTopicsResult createTopicsResult = adminClient.createTopics(topics);
			
			createTopicsResult.all()
				.get();
			
		} catch (InterruptedException | ExecutionException e) {
			if (!(e.getCause() instanceof TopicExistsException)) {
				System.err.println("Problem in JCLTopic.create()");
                e.printStackTrace();
            }
			
			adminClient.close();
		}
	}
	
	public boolean exists(final Properties properties) {
		boolean topicExists = true;
		String topicName = properties.getProperty("topic.name");
		
		if(!serverTopicsSet.contains(topicName)) {
			try {
				serverTopicsSet = adminClient.listTopics().names().get();
				topicExists = serverTopicsSet.contains(topicName);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		return topicExists;
	}
}
