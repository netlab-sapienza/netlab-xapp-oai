#include "../src/xapp-agent/agent_connector.hpp"

#include <cassert>
#include <cstring>
#include <thread>

int agent_socket_fd = -1;
std::vector<std::string> gnb_id_vector;

static void read_all(int fd, char* data, size_t size) {
  while (size) {
    const ssize_t count = recv(fd, data, size, 0);
    assert(count > 0);
    data += count;
    size -= static_cast<size_t>(count);
  }
}

int main() {
  int fds[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
  agent_socket_fd = fds[0];

  const char report[] = {'a', '\0', 'b'};
  assert(send_socket(report, sizeof(report)) == 0);
  uint32_t wire_length = 0;
  read_all(fds[1], reinterpret_cast<char*>(&wire_length), sizeof(wire_length));
  assert(ntohl(wire_length) == sizeof(report));
  char received[sizeof(report)];
  read_all(fds[1], received, sizeof(received));
  assert(memcmp(received, report, sizeof(report)) == 0);

  const uint32_t length = htonl(sizeof(report));
  std::thread writer([&]() {
    const char* prefix = reinterpret_cast<const char*>(&length);
    for (size_t i = 0; i < sizeof(length); ++i)
      assert(send(fds[1], prefix + i, 1, 0) == 1);
    for (char c : report)
      assert(send(fds[1], &c, 1, 0) == 1);
  });
  std::vector<char> frame;
  assert(receive_socket(frame));
  writer.join();
  assert(frame.size() == sizeof(report));
  assert(memcmp(frame.data(), report, sizeof(report)) == 0);

  const uint32_t oversized = htonl(MAX_AGENT_FRAME_BYTES + 1);
  assert(send(fds[1], &oversized, sizeof(oversized), 0) == sizeof(oversized));
  assert(!receive_socket(frame));
  close(fds[0]);
  close(fds[1]);
}
