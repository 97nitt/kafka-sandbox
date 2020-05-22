package sandbox.kafka.requestreply;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import sandbox.kafka.requestreply.RequestReplyTests.Initializer;
import sandbox.kafka.requestreply.users.User;
import sandbox.kafka.requestreply.users.UserResponse;
import sandbox.kafka.test.KafkaIntegrationTest;

/**
 * Integration tests verifying synchronous requests to the Users API, backed by Kafka.
 *
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = Initializer.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RequestReplyTests extends KafkaIntegrationTest {

  /**
   * Custom Spring application context initializer to override the default Kafka broker and schema
   * registry configuration with URLs for Docker containers started via Testcontainers.
   *
   */
  static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(@NotNull ConfigurableApplicationContext applicationContext) {
      TestPropertyValues.of(
          "kafka.brokers=" + kafka.getBootstrapServers(),
          "kafka.schema-registry=" + schemaRegistryUrl)
          .applyTo(applicationContext);
    }
  }

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  @Order(1)
  public void createUser() {
    // create new user
    User user = new User();
    user.setEmail("donkey.kong@email.com");
    user.setFirstName("donkey");
    user.setLastName("kong");

    ResponseEntity<UserResponse> response = restTemplate.exchange(
        String.format("http://localhost:%s/users", port),
        HttpMethod.POST,
        new HttpEntity<>(user),
        UserResponse.class);

    // verify response
    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());

    UserResponse body = response.getBody();
    assertNotNull(body.getUser().getId());
    assertEquals(user.getEmail(), response.getBody().getUser().getEmail());
    assertEquals(user.getFirstName(), response.getBody().getUser().getFirstName());
    assertEquals(user.getLastName(), response.getBody().getUser().getLastName());
    assertEquals("Successfully saved entity", response.getBody().getMessage());

    // verify persisted data
    verifyUser(body.getUser().getId(), 200, body.getUser());
  }

  @Test
  @Order(2)
  public void updateUser() {
    // get user created in previous test
    User user = getUser();

    // update user
    user.setEmail("donkey.kong@gmail.com");
    user.setFirstName("Donkey");
    user.setLastName("Kong");

    ResponseEntity<UserResponse> response = restTemplate.exchange(
        String.format("http://localhost:%s/users/%s", port, user.getId()),
        HttpMethod.PUT,
        new HttpEntity<>(user),
        UserResponse.class);

    // verify response
    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());
    assertEquals(user, response.getBody().getUser());
    assertEquals("Successfully saved entity", response.getBody().getMessage());

    // verify persisted data
    verifyUser(user.getId(), 200, response.getBody().getUser());
  }

  @Test
  @Order(3)
  public void deleteUser() {
    // get user created in previous test
    User user = getUser();

    // delete user
    ResponseEntity<UserResponse> response = restTemplate.exchange(
        String.format("http://localhost:%s/users/%s", port, user.getId()),
        HttpMethod.DELETE,
        null,
        UserResponse.class);

    // verify response
    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());
    assertEquals(user, response.getBody().getUser());
    assertEquals("Successfully deleted entity", response.getBody().getMessage());

    // verify persisted data
    verifyUser(user.getId(), 404, null);
  }

  private User getUser() {
    ResponseEntity<User[]> response = restTemplate.getForEntity(
        String.format("http://localhost:%s/users", port),
        User[].class);

    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());
    assertEquals(1, response.getBody().length);
    return response.getBody()[0];
  }

  private void verifyUser(String id, int status, User expected) {
    ResponseEntity<User> response = restTemplate.getForEntity(
        String.format("http://localhost:%s/users/%s", port, id),
        User.class);

    assertEquals(status, response.getStatusCodeValue());
    assertEquals(expected, response.getBody());
  }
}
