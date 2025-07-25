#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <vector>
#include <unordered_set>
#include <optional>
#include <map>
#include <asio.hpp>

using asio::ip::tcp;
using namespace std::chrono;

// Forward declarations
class RedisConnection;
class BlockingOperationManager;
class EventNotificationSystem;

struct StreamEntry {
  std::string id;
  std::map<std::string, std::string> field;
  StreamEntry(const std::string& entry_id, const std::map<std::string, std::string>& entry_field)
    : id(entry_id), field(entry_field) {}
};

class RedisStream {
private:
  std::vector<StreamEntry> entries_;
  std::string last_id_;
  std::uint64_t sequence_counter_;
public:
  RedisStream()
    :last_id_("0-0"), sequence_counter_(1) {};
  struct CompareEntryByID {
    bool operator()(const std::string& id, const StreamEntry& entry) {
      return !is_id_greater_or_equal(id, entry.id);
    }
    bool operator()(const StreamEntry& entry, const std::string& id) {
      return !is_id_greater_or_equal(entry.id, id);
    }
  };
  std::string get_last_id() {
    return last_id_;
  }
  std::string add_entry(const std::string& id, const std::map<std::string, std::string>& fields) {
    std::string actual_id = id;
    if ( id == "*" ) {
      auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
      auto last_id_time_stamp_part = split_id(last_id_).first;
      if (last_id_time_stamp_part != static_cast<uint64_t>(now)) {
        sequence_counter_ = 0;
      }
      actual_id = std::to_string(now) + "-" + std::to_string(sequence_counter_++);
    } else if ( id.find("-*") != std::string::npos ) {
      actual_id.pop_back();
      auto time_stamp_part = std::stoull(actual_id.substr(0, actual_id.find("-")));
      auto last_id_time_stamp_part = split_id(last_id_).first;
      if (time_stamp_part != last_id_time_stamp_part) {
        sequence_counter_ = 0;
      }
      actual_id.append(std::to_string(sequence_counter_++));
    }
    if (!is_id_greater(actual_id, last_id_)) {
      return "";
    }
    entries_.emplace_back(actual_id, fields);
    last_id_ = actual_id;
    return actual_id;
  }
  std::vector<StreamEntry> get_range(const std::string& start_id, const std::string& end_id) {
    CompareEntryByID comp;
    std::vector<StreamEntry>::iterator start_it = entries_.begin();
    std::vector<StreamEntry>::iterator end_it = entries_.end();
    if (start_id != "-") {
      start_it = std::lower_bound(entries_.begin(), entries_.end(), start_id, comp);
    }
    if (end_id != "+") {
      end_it = std::upper_bound(entries_.begin(), entries_.end(), end_id, comp);
    }
    std::vector<StreamEntry> result;
    result.assign(start_it, end_it);
    return result;
  }
private:
  static bool is_id_greater(const std::string& id1, const std::string& id2) {
    auto part1 = split_id(id1);
    auto part2 = split_id(id2);
    if (part1.first > part2.first) { return true; }
    if (part1.first < part2.first) { return false; }
    // becaues the id forms like -*, "*" is incremented, so we assert they won't be equal;
    return part1.second > part2.second;
  }
  static bool is_id_greater_or_equal(const std::string& id1, const std::string& id2) {
    auto part1 = split_id(id1);
    auto part2 = split_id(id2);
    return part1 == part2 || is_id_greater(id1, id2);
  }
  static std::pair<std::uint64_t, std::uint64_t> split_id(const std::string& id) {
    std::size_t dash_pos = id.find("-");
    if ( dash_pos == std::string::npos ) {
      return {std::stoull(id), 0};
    }
    return {std::stoull(id.substr(0, dash_pos)), std::stoull(id.substr(dash_pos+1))};
  }
};

// basic interface for blocking operations
class BlockingOperation {
public:
  enum class Type { BLPOP };
  virtual ~BlockingOperation() = default;
  virtual Type get_type() const = 0;
  virtual bool is_expired() const = 0;
  virtual std::vector<std::string> get_watched_keys() const = 0;
  virtual void handle_timeout() = 0;
  virtual bool try_execute() = 0;
protected:
  std::shared_ptr<RedisConnection> connection_;
  steady_clock::time_point start_time_;
  int timeout_milliseconds_;
public:
  BlockingOperation(std::shared_ptr<RedisConnection> conn, int timeout)
    :connection_(conn), start_time_(steady_clock::now()), timeout_milliseconds_(timeout) {}
  std::shared_ptr<RedisConnection> get_connection() const { return connection_; }
  bool is_timeout_enabled() const { return timeout_milliseconds_ > 0; }
  std::chrono::milliseconds get_remaining_time() const {
    if (!is_timeout_enabled()) {
      return milliseconds::max(); // No timeout
    }
    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start_time_);
    return milliseconds(timeout_milliseconds_) - elapsed;
  }
};

class BlpopOperation : public BlockingOperation,
                       public std::enable_shared_from_this<BlpopOperation> {
private:
  std::vector<std::string> keys_;
public:
  BlpopOperation(std::shared_ptr<RedisConnection> conn, const std::vector<std::string>& keys, int timeont)
    :BlockingOperation(conn, timeont), keys_(keys) {}
  Type get_type() const override { return Type::BLPOP; }
  bool is_expired() const override {
    if (!is_timeout_enabled()) {
      return false; // No timeout
    }
    return get_remaining_time() <= seconds(0);
  }
  std::vector<std::string> get_watched_keys() const override { return keys_; }
  void handle_timeout() override;
  bool try_execute() override;
};

class EventNotificationSystem {
public:
  enum class EventType { LIST_PUSH, LIST_POP };
  struct Event{
    EventType type;
    std::string key;
    std::vector<std::string> values;
    Event(EventType type, const std::string& key, const std::vector<std::string>& values = {})
      :type(type), key(key), values(values) {}
  };
private:
  std::unordered_map<std::string, std::vector<std::weak_ptr<BlockingOperation>>> key_watchers_;
  std::unordered_map<EventType, std::vector<std::function<void(const Event&)>>> event_handlers_;
public:
  void register_blocking_operation(const std::shared_ptr<BlockingOperation>& op) {
    auto weak_op = std::weak_ptr<BlockingOperation>(op);
    for (const auto& key : op->get_watched_keys()) {
      key_watchers_[key].push_back(weak_op);
    }
  }
  void unregister_blocking_operation(const std::shared_ptr<BlockingOperation>& op) {
    for (const auto& key : op->get_watched_keys()) {
      auto& watchers = key_watchers_[key];
      watchers.erase(std::remove_if(watchers.begin(), watchers.end(),
        [&op](const std::weak_ptr<BlockingOperation>& weak_op) {
         return weak_op.lock() == op || weak_op.expired();
        }), watchers.end());
      if (watchers.empty()) {
        key_watchers_.erase(key); // Remove key if no watchers left
      }
    }
  }
  void emit_event(const Event& event) {
    auto it = key_watchers_.find(event.key);
    if (it != key_watchers_.end()) {
      auto& watchers = it->second;
      std::vector<std::shared_ptr<BlockingOperation>> relevant_ops;
      for (auto& weak_op : watchers) {
        if (auto op = weak_op.lock()) {
          if (is_event_relevant_operation(event, op)) {
            relevant_ops.push_back(op);
          }
        }
      }
      if (!relevant_ops.empty()) {
        auto op = relevant_ops.front();
        if (op->try_execute()) {
          unregister_blocking_operation(op);
        }
      }
    }
    cleanup_expired_watchers(event.key);
    auto handler_it = event_handlers_.find(event.type);
    if (handler_it != event_handlers_.end()) {
      for (auto& handler : handler_it->second) {
        handler(event);
      }
    }
  }
  void register_event_handler(EventType type, std::function<void(const Event&)> handler) {
    event_handlers_[type].push_back(handler);
  }
private:
  bool is_event_relevant_operation(const Event& event, const std::shared_ptr<BlockingOperation>& op) {
    switch (event.type) {
      case EventType::LIST_PUSH:
        return op->get_type() == BlockingOperation::Type::BLPOP;
      default:
        return false; // Unsupported event type
    }
  }
  void cleanup_expired_watchers(const std::string key) {
    auto it = key_watchers_.find(key);
    if ( it == key_watchers_.end() ) {
      return;
    }
    auto watchers = it->second;
    watchers.erase(std::remove_if(watchers.begin(), watchers.end(),
      [](const std::weak_ptr<BlockingOperation> weak_op) {
        return weak_op.expired();
      }), watchers.end());
    if (watchers.empty()) {
      key_watchers_.erase(key);
    }
  }
};

class BlockingOperationManager {
private:
  std::vector<std::shared_ptr<BlockingOperation>> active_operations_;
  std::shared_ptr<EventNotificationSystem> event_systems_;
  asio::steady_timer cleanup_timer_;
public:
  BlockingOperationManager(asio::io_context& io_context, std::shared_ptr<EventNotificationSystem> event_system)
    :event_systems_(event_system), cleanup_timer_(io_context) {
      schedule_cleanup();
  };
  void add_blocking_operation(std::shared_ptr<BlockingOperation> op) {
    active_operations_.push_back(op);
    event_systems_->register_blocking_operation(op);
    if (op->is_timeout_enabled()) {
      auto timer = std::make_shared<asio::steady_timer>(cleanup_timer_.get_executor(), op->get_remaining_time());
      timer->async_wait([this, op, timer](asio::error_code ec){
        if (!ec && !op->is_expired()) {
          op->handle_timeout();
          // remove_blocking logic
          remove_blocking_operation(op);
        }
      });
    }
  }
  void remove_blocking_operation(std::shared_ptr<BlockingOperation> op) {
    event_systems_->unregister_blocking_operation(op);
    active_operations_.erase(
      std::remove(active_operations_.begin(), active_operations_.end(), op),
      active_operations_.end()
    );
  }
  void emit_event(const EventNotificationSystem::Event& event) {
    event_systems_->emit_event(event);
  }
private:
  // I think it is useless TODO: remove it
  void schedule_cleanup() {
      cleanup_timer_.expires_after(seconds(1));
      cleanup_timer_.async_wait([this](asio::error_code ec){
        if (!ec) {
          // cleanup_logic
          cleanup_expired_operations();
          schedule_cleanup();
        }
      });
  }
  void cleanup_expired_operations() {
    auto it = std::remove_if(active_operations_.begin(), active_operations_.end(), 
      [this](std::shared_ptr<BlockingOperation> op) {
        if (op->is_expired()) {
          op->handle_timeout();
          event_systems_->unregister_blocking_operation(op);
          return true;
        }
        return false;
      });
    active_operations_.erase(it, active_operations_.end());
  }
};

class RedisStore {
public:
  enum class DataType { STRING, LIST, STREAM, NONE };
private:
  std::unordered_map<std::string, std::string> data_;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> expi_;
  std::unordered_map<std::string, std::vector<std::string>> list_;
  std::unordered_map<std::string, std::unique_ptr<RedisStream>> stream_;
  std::shared_ptr<BlockingOperationManager> blocking_manager_;
  void cleanup_key(const std::string& key) { data_.erase(key); expi_.erase(key); list_.erase(key); stream_.erase(key); }
public:
  void set_blocking_manager(std::shared_ptr<BlockingOperationManager> blocking_manager) {
    blocking_manager_ = blocking_manager;
  }
  DataType get_type(const std::string& key) {
    auto expi_it = expi_.find(key);
    if (expi_it != expi_.end()) {
      if (steady_clock::now() > expi_it->second) {
        cleanup_key(key);
        return DataType::NONE;
      }
    }
    if (data_.find(key) != data_.end()) {
      return DataType::STRING;
    }
    if (list_.find(key) != list_.end()) {
      return DataType::LIST;
    }
    if (stream_.find(key) != stream_.end()) {
      return DataType::STREAM;
    }
    return DataType::NONE;
  }
  void set(const std::string& key, const std::string& value) {
    cleanup_key(key);
    data_[key] = value;
  }
  void set(const std::string& key, const std::string& value, int ttl) {
    cleanup_key(key);
    data_[key] = value;
    expi_[key] = steady_clock::now() + milliseconds(ttl);
  }
  std::string get(const std::string& key) {
    auto exp_it = expi_.find(key);
    if (exp_it != expi_.end()) {
      if (steady_clock::now() > exp_it->second) {
        data_.erase(key); // Remove expired key
        expi_.erase(exp_it); // Remove expiration entry
        return ""; // Key has expired
      }
    }
    auto data_it = data_.find(key);
    return data_it != data_.end() ? data_it->second : "";
  }
  std::size_t rpush(const std::string& key, const std::vector<std::string>::const_iterator begin, const std::vector<std::string>::const_iterator end) {
    if (get_type(key) != DataType::LIST) {
      cleanup_key(key);
    }
    auto& list = list_[key];
    list.insert(list.end(), begin, end);
    std::size_t result = list.size();
    if (blocking_manager_) {
      blocking_manager_->emit_event(
        EventNotificationSystem::Event(EventNotificationSystem::EventType::LIST_PUSH, key, std::vector<std::string>(begin, end))
      );
    }
    
    return result;
  }
  std::vector<std::string> lrange(const std::string& key, int start, int end) {
    auto it = list_.find(key);
    if (it == list_.end()) {
      return {}; // Key doesn't exist
    }
    const auto& list = it->second;
    int size = static_cast<int>(list.size());
    if (start < 0) {
      start += size; // Convert negative index to positive
      if (start < 0) {
        start = 0; // Ensure start is not negative
      }
    }
    if (end < 0) {
      end += size; // Convert negative index to positive
      if (end < 0) {
        end = 0; // Ensure end is not negative
      }
    }
    if (start > end || start >= size) {
      return {}; // Invalid range
    }
    // Redis LRANGE end is inclusive, so we need to add 1 for the iterator range
    if (end >= size) {
      end = size - 1;
    }
    return std::vector<std::string>(list.begin() + start, list.begin() + end + 1);
  }
  std::size_t lpush(const std::string& key, const std::vector<std::string>::const_iterator begin, const std::vector<std::string>::const_iterator end) {
    if (get_type(key) != DataType::LIST) {
      cleanup_key(key);
    }
    auto& list = list_[key];
    std::vector<std::string> temp(begin, end);
    std::reverse(temp.begin(), temp.end());
    list.insert(list.begin(), temp.begin(), temp.end());
    std::size_t result = list.size(); // if blpop performs, list will be poped, and size then changes
    if (blocking_manager_) {
      blocking_manager_->emit_event(
        EventNotificationSystem::Event(EventNotificationSystem::EventType::LIST_PUSH, key, temp)
      );
    }
    
    return result;
  }
  std::size_t llen(const std::string& key) {
    auto it = list_.find(key);
    if (it == list_.end()) {
      return 0; // Key doesn't exist
    }
    return it->second.size();
  }
  std::string lpop(const std::string& key) {
    auto it = list_.find(key);
    if( it != list_.end() && !it->second.empty()) {
      std::string value = it->second.front();
      it->second.erase(it->second.begin());
      if (it->second.empty()) {
        list_.erase(it); // Remove the key if the list is empty
      }
      return value;
    }
    return "";
  }
  std::vector<std::string> lpop(const std::string& key, int count) {
    if (count <= 0) {
      return {}; // Invalid count
    }
    auto it = list_.find(key);
    if (it == list_.end() || it->second.empty()) {
      return {};
    }
    if ( count > static_cast<int>(it->second.size()) ) {
      count = static_cast<int>(it->second.size()); // Adjust count if it exceeds list size
    }
    std::vector<std::string> values(it->second.begin(), it->second.begin() + count);
    it->second.erase(it->second.begin(), it->second.begin() + count);
    if (it->second.empty()) {
      list_.erase(it); // Remove the key if the list is empty
    }
    return values;
  }
  std::optional<std::pair<std::string, std::string>> try_blpop(const std::vector<std::string>& keys) {
    for (auto key: keys) {
      auto it = list_.find(key);
      if (it != list_.end() && !it->second.empty()) {
        std::string value = it->second.front();
        it->second.erase(it->second.begin());
        if (it->second.empty()) {
          list_.erase(it);
        }
        return std::make_optional(std::make_pair(key, value));
      }
    }
    return std::nullopt;
  }
  std::string xadd(const std::string& key, const std::string& id, const std::map<std::string, std::string>& fields) {
    if (get_type(key) != DataType::STREAM) {
      cleanup_key(key);
    }
    if (stream_.find(key) == stream_.end()) {
      stream_[key] = std::make_unique<RedisStream>();
    }
    std::string result = stream_[key]->add_entry(id, fields);
    return result;
  }
  std::vector<StreamEntry> xrange(const std::string& key, const std::string& start, const std::string& end) {
    auto it = stream_.find(key);
    if (it == stream_.end()) {
      return {};
    }
    auto result = it->second->get_range(start, end);
    return result;
  }
};

class RespParser {
public:
  RespParser() = default;
  static std::vector<std::string> parse(const std::string& input) {
    std::vector<std::string> result;
    // *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    if (input.empty() || input[0] != '*') {
      return result; // Not a valid Redis message
    }
    // 2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    std::size_t pos = 1;
    std::size_t end = input.find("\r\n", pos);
    if (end == std::string::npos) {
      return result; // Incomplete message
    }
    std::size_t count = std::stoul(input.substr(pos, end - pos));
    pos = end + 2; // Move past "\r\n"
    // $4\r\nECHO\r\n$3\r\nhey\r\n
    for (std::size_t i = 0; i < count; i++) {
      if(pos >= input.size()) {
        break;
      }
      if (input[pos] != '$') {
        break; // Not a valid Redis message
      }
      pos++;
      // 4\r\nECHO\r\n$3\r\nhey\r\n
      end = input.find("\r\n", pos);
      if (end == std::string::npos) {
        break; // Incomplete message
      }
      std::size_t length = std::stoul(input.substr(pos, end - pos));
      pos = end + 2; // Move past "\r\n"
      // ECHO\r\n$3\r\nhey\r\n
      if (pos + length > input.size()) {
        break; // Incomplete message
      }
      result.push_back(input.substr(pos, length));
      pos += length; // Move past the actual message
      // \r\n$3\r\nhey\r\n
      if (pos + 2 > input.size() || input[pos] != '\r' || input[pos + 1] != '\n') {
        break; // Incomplete message
      }
      pos += 2; // Move past "\r\n"
      // $3\r\nhey\r\n
    }
    return result;
  }
  static std::string format_simple_response(const std::string& input) {
    return "+" + input + "\r\n";
  }
  static std::string format_bulk_response(const std::string& input) {
    return "$" + std::to_string(input.length()) + "\r\n" + input + "\r\n";
  }
  static std::string format_null_bulk_response() {
    return "$-1\r\n";
  }
  static std::string format_integer_response(const int value) {
    return ":" + std::to_string(value) + "\r\n";
  }
  static std::string format_multi_bulk_response(const std::vector<std::string>& values) {
    std::string response = "*" + std::to_string(values.size()) + "\r\n";
    for (const auto& value: values) {
      response += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    }
    return response;
  }
  static std::string format_blpop_response(const std::pair<std::string, std::string>& pair) {
    std::string response = "*2\r\n$" + std::to_string(pair.first.length()) + "\r\n" + pair.first + "\r\n$"
                          + std::to_string(pair.second.length()) + "\r\n" + pair.second + "\r\n";
    return response;
  }
  static std::string format_simple_error_response(const std::string& error_message) {
    std::string response = "-ERR " + error_message + "\r\n";
    return response;
  }
  static std::string format_array_response(const std::vector<StreamEntry> entries) {
    std::string response;
    response = "*" + std::to_string(entries.size()) + "\r\n";
    for (const auto& entry : entries) {
      response += "*2\r\n$" + std::to_string(entry.id.length()) + "\r\n"
                + entry.id + "\r\n"
                + "*" + std::to_string(2 * entry.field.size()) + "\r\n";
      for (const auto& f : entry.field) {
        response += "$" + std::to_string(f.first.size()) + "\r\n" + f.first + "\r\n";
        response += "$" + std::to_string(f.second.size()) + "\r\n" + f.second + "\r\n";
      }
    }
    return response;
  }
};

class RedisConnection : public std::enable_shared_from_this<RedisConnection> {
private:
  tcp::socket socket_;
  std::vector<char> read_buffer_;
  std::string message_buffer_;
  std::shared_ptr<RedisStore> store_;
  std::shared_ptr<BlockingOperationManager> blocking_manager_;
  std::unordered_set<std::shared_ptr<BlockingOperation>> my_blocking_ops_;
public:
  RedisConnection(tcp::socket socket, 
                  std::shared_ptr<RedisStore> store,
                  std::shared_ptr<BlockingOperationManager> blocking_manager)
    :socket_(std::move(socket)),
    read_buffer_(1024),
    store_(store),
    blocking_manager_(blocking_manager) {
      std::cout << "Client connected\n";
  }
  ~RedisConnection() {
    for (auto op: my_blocking_ops_) {
      blocking_manager_->remove_blocking_operation(op);
    }
    std::cout << "Client disconnected\n";
  }
  void start() {
    do_read();
  }
  void send_response(const std::string& response) {
    do_write(response);
  }
  void add_blocking_operation(std::shared_ptr<BlockingOperation> op) {
    my_blocking_ops_.insert(op);
  }
  void remove_blocking_operation(std::shared_ptr<BlockingOperation> op) {
    my_blocking_ops_.erase(op);
  }
  std::optional<std::pair<std::string, std::string>> try_blpop(const std::vector<std::string>& keys) {
    return store_->try_blpop(keys);
  }
private:
  void do_read() {
    auto self = shared_from_this();
    socket_.async_read_some(
      asio::buffer(read_buffer_),
      [this, self](asio::error_code ec, std::size_t byte_read) {
        if (!ec && byte_read > 0) {
          // read_buffer_ is used duplicated
          message_buffer_.append(read_buffer_.data(), byte_read);
          do_process_messages();
          do_read();
        } else {
          if (ec != asio::error::eof) {
            std::cerr << "Read error: " << ec.message() << std::endl;
          }
          std::cout << "Client connected\n";
        }
      }
    );
  }
  void do_write(const std::string& response) {
    auto self = shared_from_this();
    asio::async_write(
      socket_,
      asio::buffer(response),
      [this, self](asio::error_code ec, std::size_t) {
        if (ec) {
          std::cerr << "Write error" << ec.message() << std::endl;
        }
      }
    );
  }
  void do_process_messages() {
    std::string response;
    while (true) {
      std::size_t complete_message_pos = do_find_complete_message();
      if (complete_message_pos == std::string::npos) {
        break; // No complete message found
      }
      std::string complete_message = message_buffer_.substr(0, complete_message_pos);
      message_buffer_.erase(0, complete_message_pos);
      do_process_command(complete_message);
    }
  }
  std::size_t do_find_complete_message() {
    // *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    if (message_buffer_.empty() || message_buffer_[0] != '*') {
      return std::string::npos; // Not a valid Redis message
    }
    // 2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    std::size_t pos = 1;
    std::size_t end = message_buffer_.find("\r\n", pos);
    if (end == std::string::npos) {
      return std::string::npos; // Incomplete message
    }
    std::size_t count = std::stoul(message_buffer_.substr(pos, end - pos));
    pos = end + 2; // Move past "\r\n"
    // $4\r\nECHO\r\n$3\r\nhey\r\n
    for (std::size_t i = 0; i < count; i++) {
      if(pos >= message_buffer_.size()) {
        return std::string::npos;
      }
      if (message_buffer_[pos] != '$') {
        return std::string::npos; // Not a valid Redis message
      }
      pos++;
      // 4\r\nECHO\r\n$3\r\nhey\r\n
      end = message_buffer_.find("\r\n", pos);
      if (end == std::string::npos) {
        return std::string::npos; // Incomplete message
      }
      std::size_t length = std::stoul(message_buffer_.substr(pos, end - pos));
      pos = end + 2; // Move past "\r\n"
      // ECHO\r\n$3\r\nhey\r\n
      if (pos + length > message_buffer_.size()) {
        return std::string::npos; // Incomplete message
      }
      pos += length; // Move past the actual message
      // \r\n$3\r\nhey\r\n
      if (pos + 2 > message_buffer_.size() || message_buffer_[pos] != '\r' || message_buffer_[pos + 1] != '\n') {
        return std::string::npos; // Incomplete message
      }
      pos += 2; // Move past "\r\n"
      // $3\r\nhey\r\n
    }
    return pos;
  }
  void do_process_command(const std::string& command) {
    auto command_parts = RespParser::parse(command);
    if (command_parts.empty())  {
      return;
    }
    std::string cmd = command_parts[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    std::string response;
    if (cmd == "PING") {
      response = RespParser::format_simple_response("PONG");
    } else if (cmd == "ECHO" && command_parts.size() >= 2) {
      response = RespParser::format_bulk_response(command_parts[1]);
    } else if (cmd == "SET" && command_parts.size() >= 3) {
      if (command_parts.size() >= 5) {
        std::transform(command_parts[3].begin(), command_parts[3].end(), command_parts[3].begin(), ::toupper);
        if (command_parts[3] == "PX") {
          int ttl = std::stoi(command_parts[4]);
          store_->set(command_parts[1], command_parts[2], ttl);
          response = RespParser::format_simple_response("OK");
        }
      } else {
        store_->set(command_parts[1], command_parts[2]);
        response = RespParser::format_simple_response("OK");
      }
    } else if (cmd == "GET" && command_parts.size() >= 2) {
      std::string value = store_->get(command_parts[1]);
      if (!value.empty()) {
        response = RespParser::format_bulk_response(value);
      } else {
        response = RespParser::format_null_bulk_response();
      }
    } else if (cmd == "RPUSH" && command_parts.size() >= 3) {
      std::size_t list_size = store_->rpush(command_parts[1], command_parts.begin()+2, command_parts.end());
      response = RespParser::format_integer_response(static_cast<int>(list_size));
    } else if (cmd == "LRANGE" && command_parts.size() >= 4) {
      int start = std::stoi(command_parts[2]);
      int end = std::stoi(command_parts[3]);
      auto values = store_->lrange(command_parts[1], start, end);
      response = RespParser::format_multi_bulk_response(values);
    } else if (cmd == "LPUSH" && command_parts.size() >= 3) {
      std::size_t list_size = store_->lpush(command_parts[1], command_parts.begin()+2, command_parts.end());
      response = RespParser::format_integer_response(static_cast<int>(list_size));
    } else if (cmd == "LLEN" && command_parts.size() >= 2) {
      std::size_t list_size = store_->llen(command_parts[1]);
      response = RespParser::format_integer_response(static_cast<int>(list_size));
    } else if (cmd == "LPOP" && command_parts.size() >= 2) {
      if (command_parts.size() == 2) {
        auto value = store_->lpop(command_parts[1]);
        if (!value.empty()) {
          response = RespParser::format_bulk_response(value);
        } else {
          response = RespParser::format_null_bulk_response();
        }
      } else {
        auto values = store_->lpop(command_parts[1], std::stoi(command_parts[2]));
        if (!values.empty()) {
          response = RespParser::format_multi_bulk_response(values);
        } else {
          response = RespParser::format_null_bulk_response();
        }
      }
    } else if (cmd == "BLPOP" && command_parts.size() >= 3) {
      double d_timeout_milliseconds = std::stod(command_parts.back()) * 1000;
      int i_timeout = static_cast<int>(d_timeout_milliseconds);
      std::vector<std::string> keys(command_parts.begin()+1, command_parts.end()-1);
      auto blpop_op = std::make_shared<BlpopOperation>(shared_from_this(), keys, i_timeout);
      if (!blpop_op->try_execute()) {
        add_blocking_operation(blpop_op);
        blocking_manager_->add_blocking_operation(blpop_op);
      }
      return;
    } else if (cmd == "TYPE" && command_parts.size() >= 2) {
      RedisStore::DataType type = store_->get_type(command_parts[1]);
      std::string type_str;
      switch (type) {
        case RedisStore::DataType::LIST : type_str = "list"; break;
        case RedisStore::DataType::STRING : type_str = "string"; break;
        case RedisStore::DataType::NONE:  type_str = "none"; break;
        case RedisStore::DataType::STREAM: type_str = "stream"; break;
        default: type_str = "none"; break;
      }
      response = RespParser::format_simple_response(type_str);
    } else if (cmd == "XADD" && command_parts.size() >= 5) {
      std::string key = command_parts[1];
      std::string id = command_parts[2];
      std::map<std::string, std::string> fields;
      for (size_t i = 3; i < command_parts.size(); i += 2) {
        if (i + 1 < command_parts.size()) {
          fields[command_parts[i]] = command_parts[i+1];
        }
      }
      std::string entry_id = store_->xadd(key, id, fields);
      if (!entry_id.empty()) {
        response = RespParser::format_bulk_response(entry_id);
      } else {
        if (id == "0-0") {
          response = RespParser::format_simple_error_response("The ID specified in XADD must be greater than 0-0");
        } else {
          response = RespParser::format_simple_error_response("The ID specified in XADD is equal or smaller than the target stream top item");
        }
      }
    } else if (cmd == "XRANGE" && command_parts.size() >= 4) {
      std::vector<StreamEntry> result = store_->xrange(command_parts[1], command_parts[2], command_parts[3]);
      response = RespParser::format_array_response(result);
    }
    do_write(response);
  }
};

void BlpopOperation::handle_timeout() {
  connection_->send_response(RespParser::format_null_bulk_response());
  connection_->remove_blocking_operation(shared_from_this());
}

bool BlpopOperation::try_execute() {
  auto result = connection_->try_blpop(keys_);
  if (result.has_value()) {
    std::string response = RespParser::format_blpop_response(result.value());
    connection_->send_response(response);
    connection_->remove_blocking_operation(shared_from_this());
    return true;
  }
  return false;
}

class RedisServer {
private:
  tcp::acceptor acceptor_;
  std::shared_ptr<RedisStore> store_;
  std::shared_ptr<EventNotificationSystem> event_system_;
  std::shared_ptr<BlockingOperationManager> blocking_manager_;
  asio::io_context& io_context_;
public:
  RedisServer(asio::io_context& io_context, int port)
    :acceptor_(io_context, tcp::endpoint(tcp::v4(), port)),
     store_(std::make_shared<RedisStore>()),
     event_system_(std::make_shared<EventNotificationSystem>()),
     blocking_manager_(std::make_shared<BlockingOperationManager>(io_context, event_system_)),
     io_context_(io_context) {
      acceptor_.set_option(tcp::acceptor::reuse_address(true));
      store_->set_blocking_manager(blocking_manager_);
      std::cout << "Waiting for a client to connect...\n";
      std::cout << "Logs from your program will appear here!\n";
      do_accept();
    }

private:
    void do_accept() {
      acceptor_.async_accept(
        [this](asio::error_code ec, tcp::socket socket) {
          if ( !ec ) {
            std::make_shared<RedisConnection>(std::move(socket), store_, blocking_manager_)->start();
          } else {
            std::cerr << "Accept error: " << ec.message() << std::endl;
          }
          do_accept();
        }
      );
    }
};

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  try {
    asio::io_context io_context;
    RedisServer server(io_context, 6379);
    io_context.run();
  } catch( std::exception& e ) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}