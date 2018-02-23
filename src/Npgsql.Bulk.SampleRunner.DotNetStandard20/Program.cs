using Microsoft.EntityFrameworkCore;
using Npgsql.Bulk.DAL;
using System;
using System.Diagnostics;
using System.Linq;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20
{
    class Program
    {
        static void Main(string[] args)
        {
            var streets = new[] { "First", "Second", "Third" };
            var codes = new[] { "001001", "002002", "003003", "004004" };
            var extraNumbers = new int?[] { null, 1, 2, 3, 5, 8, 13, 21, 34 };

            var optionsBuilder = new DbContextOptionsBuilder<BulkContext>();
            optionsBuilder.UseNpgsql("server=localhost;user id=postgres;password=qwerty;database=copy");

            var context = new BulkContext(optionsBuilder.Options);
            context.Database.ExecuteSqlCommand("TRUNCATE addresses CASCADE");
            
            var data = Enumerable.Range(0, 100000)
                .Select((x, i) => new Address()
                {
                    StreetName = streets[i % streets.Length],
                    HouseNumber = i + 1,
                    PostalCode = codes[i % codes.Length],
                    ExtraHouseNumber = extraNumbers[i % extraNumbers.Length]
                }).ToList();

            var uploader = new NpgsqlBulkUploader(context);

            var sw = Stopwatch.StartNew();
            uploader.Insert(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution inserted {data.Count} records for {sw.Elapsed }");

            data.ForEach(x => x.HouseNumber += 1);

            sw = Stopwatch.StartNew();
            uploader.Update(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed }");

            Console.ReadLine();
        }
    }
}
