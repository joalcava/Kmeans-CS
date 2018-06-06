using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Kmeans;
using Newtonsoft.Json;

namespace Client
{
    public class ClientServer
    {
        public int Port { get; }
        private string _data = null;
        private Socket _listener;
        private readonly IPEndPoint _localEndPoint;
        
        public ClientServer(IPAddress ipAddress, int port)
        {
            Port = port;
            var ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
            _localEndPoint = new IPEndPoint(ipAddress, port);
            _listener = new Socket(ipAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        }

        public Dictionary<string, string> StartListening()
        {
            var bytes = new byte[1024];
            try
            {
                _listener.Bind(_localEndPoint);
                _listener.Listen(10);
                while (true)
                {
                    Console.WriteLine("\nEsperando la orden para iniciar...");
                    var handler = _listener.Accept();
                    _data = null;

                    while (true)
                    {
                        var bytesRec = handler.Receive(bytes);
                        _data += Encoding.ASCII.GetString(bytes, 0, bytesRec);
                        if (_data.IndexOf("<EOF>", StringComparison.Ordinal) > -1)
                        {
                            break;
                        }
                    }

                    _data = _data.Substring(0, _data.Length - "<EOF>".Length);
                    var dataJson = JsonConvert.DeserializeObject<Dictionary<string, string>>(_data);

                    if (!dataJson.ContainsKey("command"))
                        throw new InvalidOperationException("Operación no esperada.");

                    if (dataJson["command"] != "start")
                        throw new InvalidOperationException("Operación no esperada.");
                    
                    Console.WriteLine("Datos recibidos : {0}", _data);
                    var msg = Encoding.ASCII.GetBytes("OK");
                    handler.Send(msg);
                    handler.Shutdown(SocketShutdown.Both);
                    handler.Close();
                    return dataJson;
                }
            }
            catch (Exception e) {
                Console.WriteLine(e.ToString());
            }
            throw new InvalidOperationException("Operación no esperada.");
        }
    }
    
    public class Client
    {
        public IPAddress ServerIp { get; set; }
        public IPAddress LocalIpAddress { get; set; }

        public Client(string serverIp)
        {
            ServerIp = IPAddress.Parse(serverIp);
            var ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in ipHostInfo.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    LocalIpAddress = ip;
                }
            }
        }

        private (IPAddress ip, int port) StartClient()
        {
            var bytes = new byte[1024];

            try
            {
                if (LocalIpAddress is null) throw new InvalidOperationException("No se pudo obtener la ip local");

                var remoteEp = new IPEndPoint(ServerIp, 11000);
                var sender = new Socket(
                    ServerIp.AddressFamily,
                    SocketType.Stream,
                    ProtocolType.Tcp
                );

                try
                {
                    sender.Connect(remoteEp);

                    Console.WriteLine(
                        "Socket conectado a {0}", sender.RemoteEndPoint);

                    var data = new Dictionary<string, string>()
                    {
                        { "command", "join" },
                        { "ip", LocalIpAddress.MapToIPv4().ToString() },
                    };
                    
                    var jsonData = 
                        JsonConvert.SerializeObject(data, Formatting.Indented) +
                        "<EOF>";
                    
                    var msg = Encoding.ASCII.GetBytes(jsonData);
                    sender.Send(msg);
                    
                    var bytesRec = sender.Receive(bytes);
                    var stringRec = Encoding.ASCII.GetString(bytes, 0, bytesRec);
                    Console.WriteLine($"Server replied = {stringRec}");

                    sender.Shutdown(SocketShutdown.Both);
                    sender.Close();

                    return (LocalIpAddress, Convert.ToInt32(stringRec));
                }
                catch (ArgumentNullException ane) {
                    Console.WriteLine("ArgumentNullException : {0}", ane);
                }
                catch (SocketException se) {
                    Console.WriteLine("SocketException : {0}", se);
                }
                catch (Exception e) {
                    Console.WriteLine("Unexpected exception : {0}", e);
                }
            }
            catch (Exception e) {
                Console.WriteLine(e.ToString());
            }

            throw new ApplicationException("El sistema a alcanzado un estado invalido.");
        }

        private void SubmitResult(int k, double ssd)
        {
            Console.WriteLine($"Intentando enviar los siguientes resultados: {k}:{ssd}");
            var bytes = new byte[1024];

            try
            {
                var remoteEp = new IPEndPoint(ServerIp, 11000);
                var sender = new Socket(
                    ServerIp.AddressFamily,
                    SocketType.Stream,
                    ProtocolType.Tcp
                );

                try
                {
                    sender.Connect(remoteEp);

                    Console.WriteLine(
                        "Socket conectado a {0}", sender.RemoteEndPoint);

                    var data = new Dictionary<string, string>()
                    {
                        { "command", "result" },
                        { "k", k.ToString() },
                        { "ssd", ssd.ToString() }
                    };
                    
                    var jsonData = 
                        JsonConvert.SerializeObject(data, Formatting.Indented) +
                        "<EOF>";
                    
                    var msg = Encoding.ASCII.GetBytes(jsonData);
                    sender.Send(msg);
                    
                    var bytesRec = sender.Receive(bytes);
                    var stringRec = Encoding.ASCII.GetString(bytes, 0, bytesRec);
                    Console.WriteLine($"Server replied with ok");

                    sender.Shutdown(SocketShutdown.Both);
                    sender.Close();
                    Console.WriteLine("Se enviaron correctamente.");
                }
                catch (ArgumentNullException ane) {
                    Console.WriteLine("ArgumentNullException : {0}", ane);
                }
                catch (SocketException se) {
                    Console.WriteLine("SocketException : {0}", se);
                }
                catch (Exception e) {
                    Console.WriteLine("Unexpected exception : {0}", e);
                }
            }
            catch (Exception e) {
                Console.WriteLine(e.ToString());
            }
        } 
        
        public static int Main(string[] args)
        {
            var serverIp = args[0];
            var client = new Client(serverIp);

            var connectionParams = client.StartClient();
            var clientServer = new ClientServer(connectionParams.ip, connectionParams.port);
            var result = clientServer.StartListening();
            var start = Convert.ToInt16(result["kDown"]);
            var end = Convert.ToInt16(result["kUp"]);
            var epsilon = Convert.ToDouble(result["epsilon"]);
            var dsDir = result["dsDir"];
            
            var kmeans = new KMeans(0, 0.1, dsDir);
            for (var k = start; k < end; k++)
            {
                var kmeansResult = kmeans.Init(k, epsilon);
                client.SubmitResult(k, kmeansResult);
            }
            return 0;
        }
    }
}
