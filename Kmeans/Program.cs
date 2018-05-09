using System;
using Kmeans;

namespace Kmeans
{
    class Program
    {
        static void Main(string[] args)
        {
            var kmeans = new KMeans(20, 0.01, "combined_data_1.txt");
            kmeans.Init();
            
            Console.WriteLine("Press <ENTER>.");
            Console.ReadKey();
        }
    }
}