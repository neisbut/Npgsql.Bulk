using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Npgsql.Bulk.DAL;
using Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Transactions;

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
            optionsBuilder.UseNpgsql(Configuration.ConnectionString);

            var context = new BulkContext(optionsBuilder.Options);
            context.Database.ExecuteSqlCommand("TRUNCATE addresses CASCADE");

            var data = Enumerable.Range(0, 100000)
                .Select((x, i) => new Address()
                {
                    StreetName = streets[i % streets.Length],
                    HouseNumber = i + 1,
                    PostalCode = codes[i % codes.Length],
                    ExtraHouseNumber = extraNumbers[i % extraNumbers.Length],
                    Duration = new NpgsqlTypes.NpgsqlRange<DateTime>(DateTime.Now, DateTime.Now)
                }).ToList();

            var uploader = new NpgsqlBulkUploader(context);

            context.Attach(data[0]);

            var sw = Stopwatch.StartNew();
            uploader.Insert(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution inserted {data.Count} records for {sw.Elapsed }");
            Trace.Assert(context.Addresses.Count() == data.Count);

            data.ForEach(x => x.HouseNumber += 1);

            sw = Stopwatch.StartNew();
            uploader.Update(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed }");

            context.Database.ExecuteSqlCommand("TRUNCATE addresses CASCADE");
            sw = Stopwatch.StartNew();
            uploader.Import(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution imported {data.Count} records for {sw.Elapsed }");

            // With transaction
            context.Database.ExecuteSqlCommand("TRUNCATE addresses CASCADE");

            using (var transaction = new TransactionScope())
            {
                uploader.Insert(data);
            }
            Trace.Assert(context.Addresses.Count() == 0);

            sw = Stopwatch.StartNew();
            uploader.Update(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed } (after transaction scope)");

            TestAsync(context, uploader, data).Wait();

            Console.ReadLine();
        }

        static async Task TestAsync(BulkContext context, NpgsqlBulkUploader uploader, List<Address> data)
        {
            Console.WriteLine("");
            Console.WriteLine("ASYNC version...");
            Console.WriteLine("");


            var sw = Stopwatch.StartNew();
            await uploader.InsertAsync(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution inserted {data.Count} records for {sw.Elapsed }");
            Trace.Assert(context.Addresses.Count() == data.Count);

            data.ForEach(x => x.HouseNumber += 1);

            sw = Stopwatch.StartNew();
            await uploader.UpdateAsync(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed }");

            context.Database.ExecuteSqlCommand("TRUNCATE addresses CASCADE");
            sw = Stopwatch.StartNew();
            await uploader.ImportAsync(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution imported {data.Count} records for {sw.Elapsed }");

            // With transaction
            context.Database.ExecuteSqlCommand("TRUNCATE addresses CASCADE");

            using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await uploader.InsertAsync(data);
            }
            Trace.Assert(context.Addresses.Count() == 0);

            sw = Stopwatch.StartNew();
            await uploader.UpdateAsync(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed } (after transaction scope)");
        }
    }
}
