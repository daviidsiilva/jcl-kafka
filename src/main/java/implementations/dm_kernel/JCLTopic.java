package implementations.dm_kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class JCLTopic {
	
	public JCLTopic() { }
	
	public void create(final Properties properties) {
		
		try (final AdminClient client = AdminClient.create(properties)) {

			final List<NewTopic> topics = new ArrayList<>();

			topics.add(
				new NewTopic(
					properties.getProperty("topic.name"),
					Integer.parseInt(properties.getProperty("topic.partitions")),
					Short.parseShort(properties.getProperty("topic.replication.factor"))
				)
			);

			client.createTopics(topics);
			
			final CreateTopicsResult createTopicsResult = client.createTopics(topics);
			
			createTopicsResult.all()
				.get();			
				
			client.close();
		} catch (InterruptedException | ExecutionException e) {
			if (!(e.getCause() instanceof TopicExistsException)) {
				System.err.println("Problem in JCLTopic().create()");
                e.printStackTrace();
            }
		}
	}
	
	public boolean exists(final Properties properties) {
		boolean topicExists = false;
		
		try (AdminClient admin = AdminClient.create(properties)) {
			topicExists = admin.listTopics()
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
