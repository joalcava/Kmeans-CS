using System.Net.Sockets;
using System.Text;

namespace Sockets
{
    public class StateObject
    {
        // Client  socket.
        public Socket WorkSocket = null;

        // Size of receive buffer.
        public const int BufferSize = 1024;

        // Receive buffer.
        public readonly byte[] Buffer = new byte[BufferSize];

        // Received data string.
        public readonly StringBuilder Sb = new StringBuilder();
    }
}