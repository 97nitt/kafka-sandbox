package sandbox.kafka.requestreply.users;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserResponse {

  private boolean success;
  private String message;
  private User user;
}
