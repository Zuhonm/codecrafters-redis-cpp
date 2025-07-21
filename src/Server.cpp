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
  void do_process_messages() {
    std::string response = "+PONG\r\n";
    do_write(response);
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
        }
      );
      do_accept();
    }
};

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  // int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  // if (server_fd < 0) {
  //  std::cerr << "Failed to create server socket\n";
  //  return 1;
  // }
  
  // // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // // ensures that we don't run into 'Address already in use' errors
  // int reuse = 1;
  // if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
  //   std::cerr << "setsockopt failed\n";
  //   return 1;
  // }
  
  // struct sockaddr_in server_addr;
  // server_addr.sin_family = AF_INET;
  // server_addr.sin_addr.s_addr = INADDR_ANY;
  // server_addr.sin_port = htons(6379);
  
  // if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
  //   std::cerr << "Failed to bind to port 6379\n";
  //   return 1;
  // }
  
  // int connection_backlog = 5;
  // if (listen(server_fd, connection_backlog) != 0) {
  //   std::cerr << "listen failed\n";
  //   return 1;
  // }
  
  // struct sockaddr_in client_addr;
  // int client_addr_len = sizeof(client_addr);
  // std::cout << "Waiting for a client to connect...\n";

  // // You can use print statements as follows for debugging, they'll be visible when running tests.
  // std::cout << "Logs from your program will appear here!\n";

  // // Uncomment this block to pass the first stage
  // // 
  // int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
  // while (true) {
  //   std::vector<char> readbuffer(1024);
  //   int byte_read = recv(client_fd, readbuffer.data(), readbuffer.size(), 0);
  //   if (byte_read <= 0) {
  //     break;
  //   }
  //   std::string request(readbuffer.data());
  //   if (request.find("PING") != std::string::npos) {
  //     std::string resPong = "+PONG\r\n";
  //     send(client_fd, resPong.c_str(), resPong.length(), 0);
  //   }
  // }
  // close(client_fd);
  // std::cout << "Client connected\n";
  
  // close(server_fd);

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