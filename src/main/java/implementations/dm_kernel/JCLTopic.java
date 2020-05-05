package implementations.dm_kernel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

public class JCLTopic {
	public void createTopic(final Properties properties) {
		final Map<String, Object> config = new HashMap<>();

		config.put("bootstrap.servers", properties.getProperty("bootstrap.servers"));
		
		try (final AdminClient client = AdminClient.create(config)) {

			final List<NewTopic> topics = new ArrayList<>();

			topics.add(
				new NewTopic(
					properties.getProperty("topic.name"),
					Integer.parseInt(properties.getProperty("topic.partitions")),
					Short.parseShort(properties.getProperty("topic.replication.factor"))
				)
			);

			client.createTopics(topics);
		}
	}
}
