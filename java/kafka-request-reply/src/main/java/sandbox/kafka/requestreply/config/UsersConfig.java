package sandbox.kafka.requestreply.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import sandbox.kafka.requestreply.common.DeferredResultService;
import sandbox.kafka.requestreply.common.EntityListener;
import sandbox.kafka.requestreply.common.EntityService;
import sandbox.kafka.requestreply.common.LocalStore;
import sandbox.kafka.requestreply.common.ResponseListener;
import sandbox.kafka.requestreply.common.Store;
import sandbox.kafka.requestreply.users.User;
import sandbox.kafka.requestreply.users.UserController;
import sandbox.kafka.requestreply.users.UserResponse;
import sandbox.kafka.requestreply.users.UserResponseBuilder;

/**
 * Configuration class for services supporting the Users API.
 *
 */
@Configuration
public class UsersConfig {

  @Bean
  public Store<String, User> userStore() {
    return new LocalStore<>();
  }

  @Bean
  public EntityService<String, User, UserResponse> userService(
      KafkaProperties kafkaProperties,
      Producer<String, User> userProducer,
      Store<String, User> userStore,
      DeferredResultService deferredResults) {

    return new EntityService<>(
        kafkaProperties,
        userProducer,
        userStore,
        deferredResults);
  }

  @Bean
  public EntityListener<String, User, UserResponse> userListener(
      KafkaProperties kafkaProperties,
      Consumer<String, User> userConsumer,
      Store<String, User> userStore,
      Producer<String, UserResponse> userResponseProducer) {

    return new EntityListener<>(
        kafkaProperties,
        userConsumer,
        userStore,
        userResponseProducer,
        new UserResponseBuilder());
  }

  @Bean
  public ResponseListener<String, UserResponse> userResponseListener(
      Consumer<String, UserResponse> userResponseConsumer,
      DeferredResultService deferredResults) {

    return new ResponseListener<>(userResponseConsumer, deferredResults);
  }

  @Bean
  public UserController userController(EntityService<String, User, UserResponse> userService) {
    return new UserController(userService);
  }
}
