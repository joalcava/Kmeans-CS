using System;

namespace Kmeans
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var kmeans = new KMeans(5, 0.1, "/home/joalcava/Documentos/combined_data_1.txt");
            kmeans.Init();

            Console.WriteLine("DONE.");
//            Console.WriteLine("Press <ENTER>.");
//            Console.ReadKey();
        }
    }
}