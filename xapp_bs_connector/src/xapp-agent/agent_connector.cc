#include "agent_connector.hpp"
#include <cerrno>
#include <chrono>
#include <mutex>
#include <thread>

namespace {

std::mutex send_mutex;

bool send_all(int fd, const char* data, size_t size) {
  while (size > 0) {
    const ssize_t sent = send(fd, data, size, MSG_NOSIGNAL);
    if (sent < 0 && errno == EINTR) continue;
    if (sent <= 0) return false;
    data += sent;
    size -= static_cast<size_t>(sent);
  }
  return true;
}

bool receive_all(int fd, char* data, size_t size) {
  while (size > 0) {
    const ssize_t received = recv(fd, data, size, 0);
    if (received < 0 && errno == EINTR) continue;
    if (received <= 0) return false;
    data += received;
    size -= static_cast<size_t>(received);
  }
  return true;
}

}  // namespace


// open client of control socket with agent
int open_control_socket_agent(const char* dest_ip, const int dest_port) {

  std::cout << "Opening control socket with host " << dest_ip << ":" << dest_port << std::endl;

  struct sockaddr_in dest_addr = {0};
  dest_addr.sin_family = AF_INET;
  dest_addr.sin_port = htons(dest_port);

  // convert dest_ip from char* to network address
  if (inet_pton(AF_INET, dest_ip, &dest_addr.sin_addr) <= 0) {
      std::cout << "ERROR CONVERTING IP TO INTERNET ADDR" << std::endl;
      return -2;
  }

  int control_sckfd = -1;
  // Python sends the initial subscription before opening its listener. A
  // single connect can therefore race that listener and permanently lose KPMs.
  for (int attempt = 0; attempt < 50; ++attempt) {
    control_sckfd = socket(AF_INET, SOCK_STREAM, 0);
    if (control_sckfd < 0) {
      std::cout << "ERROR: OPEN SOCKET" << std::endl;
      return -1;
    }
    int option(1);
    setsockopt(control_sckfd, SOL_SOCKET, SO_REUSEADDR, (char*)&option, sizeof(option));
    if (connect(control_sckfd, (struct sockaddr *) &dest_addr, sizeof(dest_addr)) == 0) {
      break;
    }
    close(control_sckfd);
    control_sckfd = -1;
    if (attempt < 49) std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  if (control_sckfd < 0) {
    std::cout << "ERROR: CONNECT after 50 attempts" << std::endl;
    return -3;
  }

  // update map
  std::string agent_ip;
  agent_ip.assign(dest_ip);
  std::cout << "Agent IP " << agent_ip << std::endl;
  agent_socket_fd = control_sckfd;

  return 0;
}


// close control sockets
void close_control_socket_agent(void) {

  std::cout << "Closing control sockets with agent(s)" << std::endl;

  if (agent_socket_fd > -1) {
    close(agent_socket_fd);
  }

  // reset value
  agent_socket_fd = -1;
}


// find agent IP of socket to use with gNB id
void add_gnb_to_vector_unique(unsigned char* gnb_id_trans) {

  std::vector<std::string>::iterator it_gnb;

  // convert transaction identifier (unsigned char*) to string
  std::string gnb_id(reinterpret_cast<char*>(gnb_id_trans));

  // check if gnb_id is in vector and insert it if not present
  for (it_gnb = gnb_id_vector.begin(); it_gnb != gnb_id_vector.end(); ++it_gnb) { ; }

  // insert gnb_id into vector if not already present
  if (it_gnb == gnb_id_vector.end()) {
    gnb_id_vector.push_back(gnb_id);
    std::cout << "Added gNB ID " << gnb_id << " to data structure" << std::endl;
  }
}


// send through socket
int send_socket(const char* buf, int buf_len) {
  if (buf == nullptr || buf_len <= 0 ||
      static_cast<uint32_t>(buf_len) > MAX_AGENT_FRAME_BYTES) {
    std::cerr << "ERROR: invalid agent message length" << std::endl;
    return -1;
  }

  std::lock_guard<std::mutex> lock(send_mutex);
  if (agent_socket_fd < 0) {
    std::cout << "ERROR: Could not find socket for destination " << std::endl;
    return -1;
  }
  const uint32_t length = htonl(static_cast<uint32_t>(buf_len));
  if (!send_all(agent_socket_fd, reinterpret_cast<const char*>(&length), sizeof(length)) ||
      !send_all(agent_socket_fd, buf, static_cast<size_t>(buf_len))) {
    std::cerr << "ERROR: SEND to agent" << std::endl;
    return -2;
  }
  std::cout << "Message sent: " << buf_len << " bytes" << std::endl;
  return 0;
}


bool receive_socket(std::vector<char>& payload) {
  payload.clear();
  if (agent_socket_fd < 0) return false;
  uint32_t network_length = 0;
  if (!receive_all(agent_socket_fd, reinterpret_cast<char*>(&network_length),
                   sizeof(network_length))) return false;
  const uint32_t length = ntohl(network_length);
  if (length == 0 || length > MAX_AGENT_FRAME_BYTES) {
    std::cerr << "ERROR: invalid agent message length: " << length << std::endl;
    return false;
  }
  payload.resize(length);
  return receive_all(agent_socket_fd, payload.data(), length);
}
