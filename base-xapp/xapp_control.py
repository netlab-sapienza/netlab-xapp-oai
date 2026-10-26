import socket
import struct


MAX_FRAME_BYTES = 1024 * 1024


# open control socket
def open_control_socket(port: int, on_listen=None):

    print('Waiting for xApp connection on port ' + str(port))

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # host = socket.gethostname()
    # bind to INADDR_ANY
    try:
        server.bind(('', port))
        server.listen(5)
        if on_listen is not None:
            on_listen()
        control_sck, client_addr = server.accept()
    finally:
        server.close()
    print('xApp connected: ' + client_addr[0] + ':' + str(client_addr[1]))

    return control_sck


# send through socket
def send_socket(socket, msg: bytes):
    """Send one length-prefixed message, regardless of TCP write boundaries."""
    if not isinstance(msg, bytes):
        raise TypeError('socket message must be bytes')
    if not 0 < len(msg) <= MAX_FRAME_BYTES:
        raise ValueError('invalid socket message length')
    socket.sendall(struct.pack('!I', len(msg)) + msg)
    print('Socket sent ' + str(len(msg)) + ' bytes')


# receive data from socker
def _recv_exact(socket, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = socket.recv(remaining)
        if not chunk:
            raise ConnectionError('control socket closed during a frame')
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def receive_from_socket(socket) -> bytes:
    """Read exactly one length-prefixed message from the TCP stream."""
    (size,) = struct.unpack('!I', _recv_exact(socket, 4))
    if not 0 < size <= MAX_FRAME_BYTES:
        raise ValueError('invalid socket message length: ' + str(size))
    return _recv_exact(socket, size)

