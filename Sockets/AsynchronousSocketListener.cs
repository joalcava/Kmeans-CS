using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

// ReSharper disable RedundantAssignment
// ReSharper disable BuiltInTypeReferenceStyle
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Sockets
{
    public class AsynchronousSocketListener
    {
        // Thread signal.  
        public readonly ManualResetEvent AllDone = new ManualResetEvent(false);

        private int _portCounter = 11001;

        public readonly ConcurrentBag<Dictionary<string, string>> RequestsStore = new ConcurrentBag<Dictionary<string, string>>();
        private bool _listening;
        IPAddress ipAddress = null;

        public Task StartListeningAsync()
        {
            return Task.Run(() =>
            {
                RequestsStore.Clear();

                var ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());

                foreach (var address in ipHostInfo.AddressList)
                {
                    if (address.AddressFamily == AddressFamily.InterNetwork)
                    {
                        ipAddress = address;
                    }
                }
                if (ipAddress is null) throw new InvalidOperationException("No se pudo leer la ip local.");
                var localEndPoint = new IPEndPoint(ipAddress, 11000);

                // Create a TCP/IP socket.  
                var listener = new Socket(ipAddress.AddressFamily,
                    SocketType.Stream, ProtocolType.Tcp);

                // Bind the socket to the local endpoint and listen for incoming connections.  
                try
                {
                    listener.Bind(localEndPoint);
                    listener.Listen(100);
                    _listening = true;

                    while (_listening)
                    {
                        // Set the event to non signaled state.  
                        AllDone.Reset();

                        // Start an asynchronous socket to listen for connections.  
                        Console.WriteLine("Esperando una conexión ...");
                        listener.BeginAccept(
                            new AsyncCallback(AcceptCallback),
                            listener);

                        // Wait until a connection is made before continuing.  
                        AllDone.WaitOne();
                    }
                    listener.Close();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            });
        }

        public void StopListening()
        {
            _listening = false;
            try {
                var localEndPoint = new IPEndPoint(ipAddress, 11000);
                var sender = new Socket(ipAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(localEndPoint);
                var data = new Dictionary<string, string> { { "nd", "nd" } };
                var strData = JsonConvert.SerializeObject(data, Formatting.Indented);
                var byteArrData = Encoding.ASCII.GetBytes(strData);
                sender.Send(byteArrData);
            }
            catch (Exception)
            {
                Console.WriteLine("~");
            }
        }

        private void AcceptCallback(IAsyncResult ar)
        {
            // Signal the main thread to continue.  
            AllDone.Set();

            // Get the socket that handles the client request.  
            var listener = (Socket)ar.AsyncState;
            var handler = listener.EndAccept(ar);

            // Create the state object.  
            var state = new StateObject { WorkSocket = handler };
            handler.BeginReceive(state.Buffer, 0, StateObject.BufferSize, 0,
                ReadCallback, state);
        }

        private void ReadCallback(IAsyncResult ar)
        {
            string content = String.Empty;

            // Retrieve the state object and the handler socket  
            // from the asynchronous state object.  
            var state = (StateObject)ar.AsyncState;
            var handler = state.WorkSocket;

            // Read data from the client socket.   
            int bytesRead = 0;
            try
            {
                bytesRead = handler.EndReceive(ar);
            }
            catch (Exception) {
                Console.WriteLine(".");
            }

            if (bytesRead > 0)
            {
                // There  might be more data, so store the data received so far.  
                state.Sb.Append(Encoding.ASCII.GetString(
                    state.Buffer, 0, bytesRead));

                // Check for end-of-file tag. If it is not there, read   
                // more data.  
                content = state.Sb.ToString();
                if (content.IndexOf("<EOF>", StringComparison.Ordinal) > -1)
                {
                    content = content.Substring(0, content.Length - "<EOF>".Length);
                    var dataAsJson = JsonConvert.DeserializeObject<Dictionary<string, string>>(content);
                    var port = Interlocked.Add(ref _portCounter, 1);

                    dataAsJson.Add("port", port.ToString());
                    RequestsStore.Add(dataAsJson);
                    Send(handler, port.ToString()); // Reply the port
                    Console.WriteLine("Se atendió una conexión entrante.");
                }
                else
                {
                    // Not all data received. Get more.  
                    handler.BeginReceive(state.Buffer, 0, StateObject.BufferSize, 0,
                        new AsyncCallback(ReadCallback), state);
                }
            }
        }

        private void Send(Socket handler, string data)
        {
            // Convert the string data to byte data using ASCII encoding.  
            var byteData = Encoding.ASCII.GetBytes(data);

            // Begin sending the data to the remote device.  
            handler.BeginSend(byteData, 0, byteData.Length, 0,
                new AsyncCallback(SendCallback), handler);
        }

        private void SendCallback(IAsyncResult ar)
        {
            try
            {
                // Retrieve the socket from the state object.  
                var handler = (Socket)ar.AsyncState;

                // Complete sending the data to the remote device.  
                int bytesSent = handler.EndSend(ar);

                handler.Shutdown(SocketShutdown.Both);
                handler.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }
}