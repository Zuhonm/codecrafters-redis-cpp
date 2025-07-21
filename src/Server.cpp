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
#include <asio.hpp>

using asio::ip::tcp;
using namespace std::chrono;

class RedisStore {
private:
  std::unordered_map<std::string, std::string> data_;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> expi_;
  std::unordered_map<std::string, std::vector<std::string>> list_;
public:
  void set(const std::string& key, const std::string& value) {
    data_[key] = value;
    expi_.erase(key); // Remove any existing expiration
    list_.erase(key); // Remove any existing list
  }
  void set(const std::string& key, const std::string& value, int ttl) {
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
  std::size_t rpush(const std::string& key, const std::string& value) {
    data_.erase(key);
    expi_.erase(key);
    auto& list = list_[key];
    list_[key].push_back(value);
    return list_[key].size();
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
};

class RedisConnection : public std::enable_shared_from_this<RedisConnection> {
private:
  tcp::socket socket_;
  std::vector<char> read_buffer_;
  std::string message_buffer_;
  std::shared_ptr<RedisStore> store_;
public:
  RedisConnection(tcp::socket socket, std::shared_ptr<RedisStore> store)
    :socket_(std::move(socket)), read_buffer_(1024),
    store_(store) {
      std::cout << "Client connected\n";
    }
  void start() {
    do_read();
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
      std::size_t list_size = store_->rpush(command_parts[1], command_parts[2]);
      response = RespParser::format_integer_response(static_cast<int>(list_size));
    }
    do_write(response);
  }
};

class RedisServer {
private:
  tcp::acceptor acceptor_;
  std::shared_ptr<RedisStore> store_;
public:
  RedisServer(asio::io_context& io_context, int port)
    :acceptor_(io_context, tcp::endpoint(tcp::v4(), port)),
     store_(std::make_shared<RedisStore>()) {
      acceptor_.set_option(tcp::acceptor::reuse_address(true));

      std::cout << "Waiting for a client to connect...\n";
      std::cout << "Logs from your program will appear here!\n";
      do_accept();
    }

private:
    void do_accept() {
      acceptor_.async_accept(
        [this](asio::error_code ec, tcp::socket socket) {
          if ( !ec ) {
            std::make_shared<RedisConnection>(std::move(socket), store_)->start();
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