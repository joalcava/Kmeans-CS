using System;

namespace Kmeans
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var filePath = "";
            short k = 5;
            try
            {
                filePath = args[0];
                k = Convert.ToInt16(args[1]);
            }
            catch (Exception)
            {
                Console.WriteLine("Debe indicar el directorio del dataset");
                Console.WriteLine("Debe indicar el valor de K");
                throw;
            }
            var kmeans = new KMeans(k, 0.1, filePath);
            kmeans.Init();

            Console.WriteLine("DONE.");
        }
    }
}