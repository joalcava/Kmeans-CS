using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Newtonsoft.Json;
using Sockets;

// ReSharper disable InvertIf
// ReSharper disable MemberCanBePrivate.Global

namespace Server
{
    public class Server
    {
        private readonly AsynchronousSocketListener _asyncListener = new AsynchronousSocketListener();
        private readonly Dictionary<IPAddress, int> _clients = new Dictionary<IPAddress, int>();
        private readonly List<(ushort k, double ssd)> _results = new List<(ushort k, double ssd)>();
        private string _dsDir;
        private double _epsilon;
        private int _kDown, _kUp;

        public void SetParameters()
        {
            Console.WriteLine("Ingrese el rango de K a evaluar:");
            Console.Write("Limite inferior: ");
            var kDown = Console.ReadLine();
            try
            {
                _kDown = Convert.ToInt32(kDown);
            }
            catch (Exception)
            {
                Console.WriteLine("Entrada no valida.");
                SetParameters();
                return;
            }

            Console.Write("Limite superior: ");
            var kUp = Console.ReadLine();
            try
            {
                _kUp = Convert.ToInt32(kUp);
            }
            catch (Exception)
            {
                Console.WriteLine("Entrada no valida.");
                SetParameters();
                return;
            }

            if (_kDown > _kUp)
                throw new InvalidOperationException(
                    "Limite inferior no puede ser mayor al limite superior.");

            Console.Write("Ingrese el valor de epsilon (0.1): ");
            var epsilon = Console.ReadLine();
            try
            {
                _epsilon = Convert.ToDouble(epsilon);
            }
            catch (Exception)
            {
                Console.WriteLine("Entrada no valida.");
                SetParameters();
                return;
            }

            Console.Write("Ingrese el directorio del dataset: ");
            _dsDir = Console.ReadLine();

            Console.WriteLine("Listo! <ENTER> para continuar.");
            Console.ReadKey();
        }

        public void ListenForClients()
        {
            _asyncListener.StartListeningAsync();
        }
        
        private void LoadClients()
        {
            _asyncListener.StopListening();

            if (_asyncListener.RequestsStore.IsEmpty)
            {
                Console.WriteLine("No hay ningún cliente conectado.");
                throw new InvalidOperationException();
            }
            
            foreach (var request in _asyncListener.RequestsStore)
            {
                if (!request.ContainsKey("command")) continue;
                
                if (request["command"] == "join")
                {
                    var ip = request["ip"];
                    var port = request["port"];
                    
                    _clients.Add(IPAddress.Parse(ip), Convert.ToInt32(port));
                }
            }
        }
  
        private int PrintMenu()
        {
            var callback = "";
            var validOptions = new[] {1, 2, 3, 4};
            while (true)
            {
                Console.Clear();
                Console.WriteLine(callback);
                Console.WriteLine("1. Parametrizar las variables.");
                Console.WriteLine("2. Aceptar clientes.");
                Console.WriteLine("3. Iniciar procesamiento.");
                Console.WriteLine("4. Procesar resultados.");
                Console.WriteLine("5. Salir.");
                Console.WriteLine("Escriba el numero de la opción en cualquier momento.");
                var input = Console.ReadLine();
                try
                {
                    var opt = Convert.ToInt32(input);
                    if (!validOptions.Contains(opt))
                    {
                        callback = "Entrada incorrecta.\n";
                    }
                    else
                    {
                        Console.Clear();
                        return opt;
                    }
                }
                catch (Exception)
                {
                    callback = "Entrada incorrecta.\n";
                }
            }
        }

        public void TriggerProcessing()
        {
            LoadClients();

            if (_kDown == 0) {
                throw new InvalidOperationException(
                    "No se puede iniciar el procesamiento si no se han establecido los parámetros.");
            }
            
            var bytes = new byte[1024];
            var totalOfK = _kUp - _kDown;
            var totalOfClients = _clients.Count;
            var numberOfKForEachClient = totalOfK / totalOfClients;
            var currentK = _kDown;
            
            foreach (var client in _clients)
            {
                if (currentK > _kUp) return;
                
                var remoteEp = new IPEndPoint(client.Key, client.Value);
                var sender = new Socket(
                    client.Key.AddressFamily,
                    SocketType.Stream,
                    ProtocolType.Tcp
                );
                
                sender.Connect(remoteEp);
                Console.WriteLine($"Solicitando a <{client.Key.MapToIPv4()}> que inicie el procesamiento.");
                
                var data = new Dictionary<string, string>()
                {
                    {"command", "start"},
                    {"kDown", currentK.ToString()},
                    {"kUp", (currentK + numberOfKForEachClient).ToString()},
                    {"epsilon", _epsilon.ToString(CultureInfo.InvariantCulture)},
                    {"dsDir", _dsDir}
                };
                
                var dataJson = JsonConvert.SerializeObject(data, Formatting.Indented) + "<EOF>";
                var msg = Encoding.ASCII.GetBytes(dataJson);
                var bytesSent = sender.Send(msg);
                var bytesRec = sender.Receive(bytes);
                
                sender.Shutdown(SocketShutdown.Both);
                sender.Close();
                currentK += numberOfKForEachClient;
                Console.WriteLine("Todo transcurrió con normalidad\n");
            }
        }

        public void ListenForResults()
        {
            Console.WriteLine("\nEsperando resultados de forma asíncrona...");
            _asyncListener.StartListeningAsync();
        }

        public bool LoadResults()
        {
            _asyncListener.StopListening();
            foreach (var request in _asyncListener.RequestsStore)
            {
                if (!request.ContainsKey("command")) continue;
                
                if (request["command"] == "result")
                {
                    var k = Convert.ToUInt16(request["k"]);
                    var ssd = Convert.ToDouble(request["ssd"]);
                    var result = (k, ssd);
                    
                    _results.Add(result);
                }
            }
            if (_results.Any())
            {
                return true;
            }
            Console.WriteLine("No se encontró ningún resultado para procesar");
            return false;
        }

        private List<KeyValuePair<ushort[], double>> _bestKRange;
        
        private void CalculateElbowPoint()
        {
            var orderedResults = _results.OrderBy(r => r.k).ToArray();
            var slopes = new Dictionary<ushort[], double>();

            for (var i = 0; i < orderedResults.Length - 1; i++)
            {
                var a = orderedResults[i];
                var b = orderedResults[i + 1];
                var slope = (b.ssd - a.ssd) / (b.k - a.k);
                
                slopes.Add(new[] { a.k, b.k }, slope);
            }

            var bestKRange = new List<KeyValuePair<ushort[], double>>();
            var slopesArr = slopes.ToArray();
            var biggerSlopeDifference = 0.0;
            
            for (var i = 0; i < slopesArr.Length - 1; i++)
            {
                var currentSlope = slopesArr[i].Value;
                var nextSlope = slopesArr[i + 1].Value;
                var currentSlopeDifference = currentSlope - nextSlope;
                
                if (currentSlopeDifference > biggerSlopeDifference)
                {
                    biggerSlopeDifference = currentSlopeDifference;
                    bestKRange.Clear();
                    bestKRange.Add(slopesArr[i]);
                    bestKRange.Add(slopesArr[i + 1]);
                }
            }

            _bestKRange = bestKRange;
        }

        private void PrintResults()
        {
            if (_bestKRange is null)
            {
                Console.WriteLine("No se encontró una lista de resultados.");
                return;
            }

            Console.WriteLine("Las rectas cuya diferencia de pendientes es mayor son:");
            foreach (var pair in _bestKRange)
            {
                Console.WriteLine("\nRecta uno, compuesta de los valores de k entre: ");
                foreach (var k in pair.Key)
                {
                    Console.Write($"{k}, ");
                }

                Console.WriteLine($"Cuya pendiente es: {pair.Value}");
            }
        }
               
        public static int Main(string[] args)
        {
            var server = new Server();
            var done = false;
            
            while (!done)
            {
                var opt = server.PrintMenu();
                switch (opt)
                {
                    case 1:
                        server.SetParameters();
                        break;
                    case 2:
                        server.ListenForClients();
                        break;
                    case 3:
                        server.TriggerProcessing();
                        server.ListenForResults();
                        break;
                    case 4:
                        var canContinue = server.LoadResults();
                        if (canContinue) {
                            server.CalculateElbowPoint();
                            server.PrintResults();
                        }
                        else {
                            Console.WriteLine("No se puede continuar");
                            return 1;
                        }
                        break;
                    case 5:
                        done = true;
                        break;
                    default:
                        throw new InvalidOperationException("Opción no conocida.");
                }
            }

            return 0;
        }
    }
}