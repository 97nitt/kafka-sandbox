package sandbox.kafka.requestreply.users;

import java.util.Collection;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.async.DeferredResult;
import sandbox.kafka.requestreply.common.EntityService;

/**
 * Uses API controller.
 *
 */
@RequestMapping("/users")
public class UserController {

  private final EntityService<String, User, UserResponse> userService;

  /**
   * Constructor.
   *
   * @param userService user service
   */
  public UserController(EntityService<String, User, UserResponse> userService) {
    this.userService = userService;
  }

  @GetMapping
  public ResponseEntity<Collection<User>> getUsers() {
    return ResponseEntity.ok(userService.getAll());
  }

  @PostMapping
  @ResponseBody
  public DeferredResult<UserResponse> createUser(@RequestBody User user) {
    String id = UUID.randomUUID().toString();
    user.setId(id);
    return userService.save(id, user);
  }

  @GetMapping("/{id}")
  public ResponseEntity<User> getUser(@PathVariable String id) {
    User user = userService.getById(id);
    return (user != null) ? ResponseEntity.ok(user) : ResponseEntity.notFound().build();
  }

  @PutMapping("/{id}")
  @ResponseBody
  public DeferredResult<UserResponse> updateUser(@PathVariable String id, @RequestBody User user) {
    user.setId(id);
    return userService.save(id, user);
  }

  @DeleteMapping("/{id}")
  @ResponseBody
  public DeferredResult<UserResponse> deleteUser(@PathVariable String id) {
    return userService.delete(id);
  }
}
