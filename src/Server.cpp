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
};

class RedisClient : public std::enable_shared_from_this<RedisClient> {
private:
  tcp::socket socket_;
  std::vector<char> read_buffer_;
  std::string message_buffer_;
public:
  RedisClient(tcp::socket socket)
    :socket_(std::move(socket)), read_buffer_(1024) {
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
    }
    do_write(response);
  }
};

class RedisServer {
private:
  tcp::acceptor acceptor_;
public:
  RedisServer(asio::io_context& io_context, int port)
    :acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
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
            std::make_shared<RedisClient>(std::move(socket))->start();
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