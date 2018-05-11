using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kmeans
{
    internal struct Point
    {
        internal double Norm { get; set; }
        internal IDictionary<ushort, double> Rates;

        internal void CalculateNorm()
        {
            var norm = 0.0;
            foreach (var val in Rates)
            {
                norm += Math.Pow(val.Value, 2);
            }

            this.Norm = Math.Sqrt(norm);
        }
    }
    
    
    public class KMeans
    {
        private const uint DsSize = 470758;
        private readonly short _k;
        private readonly double _error;
        private readonly string _fileName;
        private readonly ushort[] _clustering;
        private readonly Point[] _ds;
        private readonly Point[] _centroids;

        
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
            
            _ds = new Point[DsSize];
            _centroids = new Point[k];            
            _clustering = new ushort[DsSize];
        }

        
        public void Init()
        {
            LoadNetflixDataset();
            GC.Collect();
            GC.WaitForPendingFinalizers();

            Parallel.For(0, DsSize, i =>
            {
                _ds[i].CalculateNorm();
            });

            SetCentroidsRandomly();
            IterateUntilConvergence();
            PrintResults();
        }

        
        private void LoadNetflixDataset()
        {
            var file = new StreamReader(_fileName);
            string line;
            ushort movie = 0;
            int counter = 0, userIndex = 0;
            var usersIndexes = new Dictionary<uint, int>((int) DsSize);
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
                        _ds[counter] = new Point
                        {
                            Rates = new Dictionary<ushort, double>()
                        };
                        usersIndexes[user] = counter;
                        userIndex = counter;
                        counter++;
                    }
                    _ds[userIndex].Rates[movie] = rate;
                }
            }

            file.Close();
            Console.WriteLine($"\nNumero de peliculas: {movie}");
            Console.WriteLine($"Numero de usuarios: {userIndex}\n\n");
        }


        private void SetCentroidsRandomly()
        {
            var rd = new Random();
            for (var i = 0; i < _k; i++)
            {
                var index = rd.Next(_ds.Length);
                var point = _ds[index];
                point.Rates = new ConcurrentDictionary<ushort, double>(point.Rates);
                _centroids[i] = point;
            }
        }
       
        
        private double Clustering()
        {
            var ssd = 0.0;

            Parallel.For(0, DsSize, i =>
            {
                ushort centroid;
                double distance;
                (centroid, distance) = ClosestCentroidTo(_ds[i]);
                _clustering[i] = centroid;
                InterlockedDoubleAdd(ref ssd, distance);
            });

            return ssd;
        }


        private (ushort, double) ClosestCentroidTo(Point point)
        {
            var distance = double.MaxValue;
            ushort ci = 0; // Centroid index

            for (ushort i = 0; i < _centroids.Length; i++)
            {
                var distancePrev = GetDistance(point, _centroids[i]);
                if (distancePrev < distance)
                {
                    distance = distancePrev;
                    ci = i;
                }
            }

            return (ci, distance);
        }


        private static double GetDistance(Point a, Point b)
        {
            var dotProduct = 0.0;
            
            var aCount = a.Rates.Count;
            var bCount = b.Rates.Count;

            var aRates = aCount < bCount ? ref a.Rates : ref b.Rates;
            var bRates = aCount < bCount ? ref b.Rates : ref a.Rates;
            
            foreach (var i in aRates)
            {
                if (bRates.TryGetValue(i.Key, out var bValue))
                {
                    dotProduct += i.Value * bValue;
                }
            }

            var normProduct = a.Norm * b.Norm;
            var thetaCosine = dotProduct / normProduct;

            return Math.Asin(thetaCosine);
        }
        
        
        private void NewCentroids()
        {
            for (var i = 0; i < _centroids.Length; i++)
            {
                _centroids[i].Rates = new ConcurrentDictionary<ushort, double>();
                _centroids[i].Norm = 0.0;
            }

            var count = new int[_k];

            Parallel.For(0, DsSize, i =>
            {
                var ci = _clustering[i];

                foreach (var pair in _ds[i].Rates)
                {
                    if (_centroids[ci].Rates 
                        is ConcurrentDictionary<ushort, double> rates)
                    {
                        rates.AddOrUpdate(
                            pair.Key,
                            pair.Value,
                            (key, oldValue) => oldValue + pair.Value);
                    }
                }

                Interlocked.Increment(ref count[ci]);
            });

            for (var i = 0; i < _centroids.Length; i++)
            {
                var keys = new List<ushort>(_centroids[i].Rates.Keys);
                foreach (var cKey in keys)
                {
                    _centroids[i].Rates[cKey] /= count[i];
                }
            }

            for (var i = 0; i < _centroids.Length; i++)
            {
                _centroids[i].CalculateNorm();
            }
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
                ssd = Clustering();
                Console.WriteLine($"SSD: {ssd:N3}");
                NewCentroids();
                iter++;
                d = Math.Abs(ssdPrev - ssd);
                Console.WriteLine($"---> {d}");
            } while (d > _error);
        }

        
        private void PrintResults()
        {
            
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
