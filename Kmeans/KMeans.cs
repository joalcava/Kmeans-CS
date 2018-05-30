using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kmeans
{
    public class KMeans
    {
        private readonly object syncRoot = new object();
        private const int _dsSize = 470758;
        private readonly short _k;
        private readonly double _error;
        private readonly string _fileName;
        private readonly IDictionary<ushort, double>[] _ds;
        private readonly IDictionary<ushort, double>[] _centroids;
        private readonly double[] _dsNorms;
        private readonly double[] _centroidsNorms;
        
        private readonly List<IDictionary<ushort, double>>[] _clustering;

        
        public KMeans(short k, double error, string fileName)
        {
            if (!File.Exists(fileName))
            {
                throw new InvalidOperationException("El archivo no existe.");
            }
            Console.WriteLine("El archivo existe!");

            _k = k;
            _error = error;
            _fileName = fileName;
            
            _ds = new IDictionary<ushort, double>[_dsSize];
            _dsNorms = new double[_dsSize];
            
            _centroids = new IDictionary<ushort, double>[_k];            
            _centroidsNorms = new double[_k];
            
            _clustering = new List<IDictionary<ushort, double>>[_k];
            InitializeClustering();
            
            _ds.Initialize();
            _dsNorms.Initialize();
            _centroidsNorms.Initialize();
            _centroids.Initialize();
        }

        private void InitializeClustering()
        {
            for (var i = 0; i < _k; i++)
            {
                if (_clustering[i] is null)
                {
                    _clustering[i] = new List<IDictionary<ushort, double>>();
                }
                else
                {
                    _clustering[i].Clear();
                }
            }
        }

        private void CalculateDsNorms()
        {
            Parallel.For(0, _dsSize, i =>
            {
                var norm = 0.0;
                foreach (var pair in _ds[i])
                {
                    norm += Math.Pow(pair.Value, 2);
                }

                _dsNorms[i] = Math.Sqrt(norm);
            });
        }
        
        private void CalculateCentroidsNorms()
        {
            Parallel.For(0, _k, i =>
            {
                var norm = 0.0;
                foreach (var pair in _centroids[i])
                {
                    norm += Math.Pow(pair.Value, 2);
                }

                _centroidsNorms[i] = Math.Sqrt(norm);
            });
        }
        
        public void Init()
        {
            LoadNetflixDataset();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            
            CalculateDsNorms();
            
            SetCentroidsRandomly();
            CalculateCentroidsNorms();

            IterateUntilConvergence();
            PrintResults();
        }
 
        private void LoadNetflixDataset()
        {
            var file = new StreamReader(_fileName);
            string line;
            ushort movie = 0;
            int counter = 0, userIndex = 0;
            var usersIndexes = new Dictionary<uint, int>((int) _dsSize);
            while ((line = file.ReadLine()) != null)
            {
                if (line.EndsWith(':'))
                {
                    movie = Convert.ToUInt16(line.Substring(0, line.Length - 1));
                    Console.WriteLine($"Leyendo datos de la película: {movie}");
                }
                else
                {
                    var elements = line.Split(',');
                    var user = Convert.ToUInt32(elements[0]);
                    var rate = Convert.ToDouble(elements[1]);

                    if (usersIndexes.ContainsKey(user))
                    {
                        userIndex = usersIndexes[user];
                    }
                    else
                    {
                        usersIndexes[user] = counter;
                        userIndex = counter;
                        counter++;
                        _ds[userIndex] = new Dictionary<ushort, double>();
                    }
                    _ds[userIndex][movie] = rate;
                }
            }

            file.Close();
            Console.WriteLine($"\nNumero de películas: {movie}");
            Console.WriteLine($"Numero de usuarios: {usersIndexes.Count}\n\n");
        }

        private void SetCentroidsRandomly()
        {
            var rd = new Random();
            for (var i = 0; i < _k; i++)
            {
                var index = rd.Next(_ds.Length);
                while (_ds[index].Count < 50)
                {
                    index = rd.Next(_ds.Length);
                }
                Console.Write($"{index}, ");
                _centroids[i] = _ds[index];
            }

            Console.WriteLine("\n");
        }
       
        private double Clustering()
        {
            var ssd = 0.0;
            
            Parallel.For(0, _ds.Length, pointIndex =>
            {
                ushort centroid;
                double distance;
                (centroid, distance) = ClosestCentroidTo(pointIndex);

                // Zona critica, para no perder datos.
                lock (syncRoot)
                {
                    _clustering[centroid].Add(_ds[pointIndex]);
                    ssd += distance;
                }
            });

            return ssd;
        }

        private (ushort, double) ClosestCentroidTo(int pointIndex)
        {
            var distance = double.MaxValue;
            ushort ci = 0; // Centroid index

            for (ushort i = 0; i < _centroids.Length; i++)
            {
                var distancePrev = GetDistance(pointIndex, i);
                if (distancePrev < distance)
                {
                    distance = distancePrev;
                    ci = i;
                }
            }

            return (ci, distance);
        }

        private double GetDistance(int aIndex, int bIndex)
        {
            var dotProduct = 0.0;
            
            foreach (var point in _ds[aIndex])
            {
                if (_centroids[bIndex].TryGetValue(point.Key, out var bValue))
                {
                    dotProduct += point.Value * bValue;
                }
            }

            var normProduct = _dsNorms[aIndex] * _centroidsNorms[bIndex];
            var thetaCosine = dotProduct / normProduct;

            return Math.Acos(thetaCosine);
        }
        
        private void NewCentroids()
        {
            for (var i = 0; i < _centroids.Length; i++)
            {
                _centroids[i] = new Dictionary<ushort, double>();
            }
            _centroidsNorms.Initialize();

            Parallel.For(0, _clustering.Length, i =>
            {
                for (var j = 0; j < _clustering[i].Count; j++)
                {
                    // No es necesario implementar una zona critica ya que
                    // el tamano del array es estatico y el diccionario solo es
                    // escrito por un hilo a la vez
                    if (_clustering[i][j] is null)
                    {
                        _clustering[i][j] = new Dictionary<ushort, double>();
                    }
                    foreach (var pair in _clustering[i][j])
                    {
                        if (_centroids[i].ContainsKey(pair.Key))
                        {
                            _centroids[i][pair.Key] += pair.Value;
                        }
                        else
                        {
                            _centroids[i][pair.Key] = pair.Value;
                        }
                    }
                }
            });

            for (var i = 0; i < _centroids.Length; i++)
            {
                var keys = new List<ushort>(_centroids[i].Keys);
                foreach (var key in keys)
                {
                    _centroids[i][key] /= _clustering[i].Count;
                }
            }
            
            CalculateCentroidsNorms();
        }

        private void IterateUntilConvergence()
        {
            var iter = 0;
            var ssd = 0.0;
            double d, ssdPrev;
            do
            {
                ssdPrev = ssd;
                Console.WriteLine($"Iteration {iter}");
                InitializeClustering();
                ssd = Clustering();
                Console.WriteLine($"SSD: {ssd}");
                d = Math.Abs(ssdPrev - ssd);
                NewCentroids();
                Console.WriteLine($"---> {d}");
                PrintResults();
                iter++;
            } while (d > _error);
        }

        private void PrintResults()
        {
            var sum = 0;
            for (var i = 0; i < _clustering.Length; i++)
            {
                Console.Write($"C{i}:{_clustering[i].Count}, ");
                sum += _clustering[i].Count;
            }
            
            Console.WriteLine($"Total: {sum}\n");
        }

        private static double InterlockedDoubleAdd(
            ref double location1, double value)
        {
            var newCurrentValue = location1;
            while (true)
            {
                var currentValue = newCurrentValue;
                var newValue = currentValue + value;
                newCurrentValue = Interlocked.CompareExchange(ref location1, newValue, currentValue);
                if (newCurrentValue == currentValue)
                    return newValue;
            }
        }
    }
}
