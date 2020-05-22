package sandbox.kafka.requestreply.users;

import sandbox.kafka.requestreply.common.ResponseBuilder;

public class UserResponseBuilder implements ResponseBuilder<User, UserResponse> {

  @Override
  public UserResponse success(User user, String message) {
    return new UserResponse(true, message, user);
  }

  @Override
  public UserResponse failed(User user, String message) {
    return new UserResponse(false, message, user);
  }
}
