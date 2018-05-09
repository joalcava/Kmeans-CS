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
        internal ConcurrentDictionary<ushort, double> Values;

        internal void CalculateNorm()
        {
            var norm = 0.0;
            foreach (var val in Values)
            {
                norm += Math.Pow(val.Value, 2);
            }

            this.Norm = Math.Sqrt(norm);
        }
    }
    
    
    public class KMeans
    {
        private const uint Size = 470758;
        private readonly short _k;
        private readonly double _error;
        private readonly string _fileName;
        private readonly ushort[] _clustering;
        private readonly Point[] _dataset;
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
            _dataset = new Point[Size];
            _clustering = new ushort[Size];
            _centroids = new Point[k];            
        }

        
        public void Init()
        {
            LoadNetflixDataset();
            GC.Collect();
            GC.WaitForPendingFinalizers();

            Parallel.For(0, Size, i =>
            {
                _dataset[i].CalculateNorm();
            });

            SetCentroidsRandomly();
            IterateUntilConvergence();

            PrintResults();
        }

        
        private void LoadNetflixDataset()
        {
            var file = new StreamReader(_fileName);
            string line;
            ushort movie=0;
            var userPos = new Dictionary<uint, int>();
            var i = 0;
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

                    int pos;
                    if (userPos.ContainsKey(user))
                    {
                        pos = userPos[user];
                    }
                    else
                    {
                        _dataset[i] = new Point
                        {
                            Values = new ConcurrentDictionary<ushort, double>()
                        };
                        userPos[user] = i;
                        pos = i;
                        i++;
                    }
                    _dataset[pos].Values[movie] = rate;
                }
            }

            file.Close();
            Console.WriteLine($"El dataset contiene {_dataset.Length} elementos \n\n");
        }


        private void SetCentroidsRandomly()
        {
            var rd = new Random();
            for (var i = 0; i < _k; i++)
            {
                var index = rd.Next(_dataset.Length);
                _centroids[i] = _dataset[index];
            }
        }


        private double Clustering()
        {
            var ssd = 0.0;

            Parallel.For(0, Size, i =>
            {
                ushort centroid;
                double distance;
                (centroid, distance) = ClosestCentroidTo(_dataset[i]);
                _clustering[i] = centroid;
                ssd += distance;
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


        private void NewCentroids()
        {
            var newCentroids = new Point [_k];
            for (var i = 0; i < newCentroids.Length; i++)
            {
                newCentroids[i].Values = new ConcurrentDictionary<ushort, double>();
            }

            var count = new int[_k];

            Parallel.For(0, Size, i =>
            {
                var ci = _clustering[i];

                foreach (var pair in _dataset[i].Values)
                {
                    newCentroids[ci].Values.AddOrUpdate(pair.Key, pair.Value, (key, oldValue) => oldValue + pair.Value);
                }

                Interlocked.Increment(ref count[ci]);
            });

            for (int i = 0; i < newCentroids.Length; i++)
            {
                foreach (var cKey in newCentroids[i].Values.Keys)
                {
                    newCentroids[i].Values[cKey] /= count[i];
                }
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
        
        
        private static double GetDistance(Point a, Point b)
        {
            var dotProduct = 0.0;

            foreach (var i in a.Values)
            {
                if (b.Values.TryGetValue(i.Key, out var bValue))
                {
                    dotProduct += i.Value * bValue;
                }
            }

            var normProduct = a.Norm * b.Norm;
            var thetaCosine = dotProduct / normProduct;

            return Math.Asin(thetaCosine);
        }
    }
}