package implementations.dm_kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import commom.Constants;
import implementations.util.JCLConfigProperties;

public class JCLTopic {
	private static JCLTopic instance;
	private static Properties topicProperties;
	private static AdminClient adminClient;
			
	private JCLTopic() {
		topicProperties = JCLConfigProperties.get(Constants.Environment.JCLKafkaConfig());
		adminClient = AdminClient.create(topicProperties);
	}
	
	public static JCLTopic getInstance() {
		if(instance != null) {
			return instance;
		}
		
		return new JCLTopic();
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
		boolean topicExists = false;
		
		try {
			topicExists = adminClient.listTopics()
				.names()
				.get()
				.stream()
				.anyMatch(
					topicName -> topicName.equalsIgnoreCase(
						properties.getProperty("topic.name")
					)
				);
			
			return topicExists;
		} catch (InterruptedException e) {
			e.printStackTrace();

			return topicExists;
		} catch (ExecutionException e) {
			e.printStackTrace();

			return topicExists;
		}
	}
}
