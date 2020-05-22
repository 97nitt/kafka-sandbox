package sandbox.kafka.requestreply;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Application {

  /**
   * Spring Boot application entry point.
   *
   * @param args application arguments
   */
  public static void main(String... args) {
    SpringApplication.run(Application.class, args);
  }
}
