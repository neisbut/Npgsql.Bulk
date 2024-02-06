using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Newtonsoft.Json;
using Npgsql.Bulk.DAL;
using Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Transactions;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20
{
    class Program
    {

        static string[] streets = new[] { "First", "Second", "Third" };

        static string[] codes = new[] { "001001", "002002", "003003", "004004" };

        static int?[] extraNumbers = new int?[] { null, 1, 2, 3, 5, 8, 13, 21, 34 };

        static void Main(string[] args)
        {
            AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

            var optionsBuilder = new DbContextOptionsBuilder<BulkContext>();
            optionsBuilder.UseNpgsql(Configuration.ConnectionString);

            var context = new BulkContext(optionsBuilder.Options);
            context.Database.Migrate();

            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");

            var data = Enumerable.Range(0, 100000)
                .Select((x, i) => new Address()
                {
                    StreetName = streets[i % streets.Length],
                    HouseNumber = i + 1,
                    PostalCode = codes[i % codes.Length],
                    ExtraHouseNumber = extraNumbers[i % extraNumbers.Length],
                    Duration = new NpgsqlTypes.NpgsqlRange<DateTime>(DateTime.UtcNow, DateTime.UtcNow),
                    AddressType = i % 2 == 0 ? AddressType.Type1 : AddressType.Type2,
                    AddressTypeInt = i % 2 == 0 ? AddressTypeInt.First : AddressTypeInt.Second,
                    UnmappedEnum = i % 2 == 0 ? UnmappedEnum.A : UnmappedEnum.B,
                }).ToList();

            var uploader = new NpgsqlBulkUploader(context);


            context.Attach(data[0]);
            data[0].AddressId = 11;

            data[1].CreatedAt = DateTime.UtcNow;

            //context.Add(data[0]);
            //context.Add(data[1]);
            //context.SaveChanges();

            // data.ForEach(x => x.StreetName = null);

            var sw = Stopwatch.StartNew();
            uploader.Insert(data);
            context.SaveChanges();
            sw.Stop();
            Console.WriteLine($"Dynamic solution inserted {data.Count} records for {sw.Elapsed }");

            // CompareValues(data, new BulkContext(optionsBuilder.Options));

            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");

            TestViaInterfaceCase(data, context);

            data.ForEach(x => x.HouseNumber += 1);

            sw = Stopwatch.StartNew();
            uploader.Update(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed }");

            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");
            sw = Stopwatch.StartNew();
            uploader.Import(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution imported {data.Count} records for {sw.Elapsed }");

            // With transaction
            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");

            using (var transaction = new TransactionScope())
            {
                uploader.Insert(data);
            }
            // Trace.Assert(context.Addresses.Count() == 0);

            sw = Stopwatch.StartNew();
            uploader.Update(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed } (after transaction scope)");

            TestAsync(context, uploader, data).Wait();

#if !NET8_0
            TestDerived(context);
#endif

            TestInsertPartialUpdateAndIndsert(context, data);

            Console.WriteLine();
            Console.WriteLine("Time to press enter to exit...");
            Console.ReadLine();
        }

        private static void CompareValues(List<Address> data, BulkContext bulkContext)
        {
            var dbData = bulkContext.Addresses.OrderBy(x => x.AddressId).ToArray();
            var local = data.OrderBy(x => x.AddressId).ToArray();

            for (var i = 0; i < dbData.Length; i++)
            {
                var dbItem = JsonConvert.SerializeObject(dbData[i]);
                var locItem = JsonConvert.SerializeObject(local[i]);
                if (dbItem != locItem)
                {
                    Console.WriteLine("Oops");
                }
            }
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
            // Trace.Assert(context.Database.ExecuteSqlRaw("SELECT COUNT(*) FROM addresses") == data.Count);

            data.ForEach(x => x.HouseNumber += 1);

            sw = Stopwatch.StartNew();
            await uploader.UpdateAsync(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed }");

            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");
            sw = Stopwatch.StartNew();
            await uploader.ImportAsync(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution imported {data.Count} records for {sw.Elapsed }");

            // With transaction
            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");

            using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await uploader.InsertAsync(data);
            }
            //Trace.Assert(context.Addresses.Count() == 0);

            sw = Stopwatch.StartNew();
            await uploader.UpdateAsync(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed } (after transaction scope)");
        }

        static void TestViaInterfaceCase<T>(IEnumerable<T> data, DbContext context) where T : IHasId
        {
            var uploader = new NpgsqlBulkUploader(context);

            var properties = data
                .First()
                .GetType()
                .GetProperties()
                .Where(x => x.GetCustomAttribute<ColumnAttribute>() != null)
                .ToArray();

            uploader.Insert(data, InsertConflictAction.UpdateProperty<T>(x => x.AddressId, properties));
        }

        static void TestDerived(BulkContext context)
        {
            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");

            var data = Enumerable.Range(0, 100000)
                .Select((x, i) => new Address2EF()
                {
                    StreetName = streets[i % streets.Length],
                    HouseNumber = i + 1,
                    PostalCode = codes[i % codes.Length],
                    ExtraHouseNumber = extraNumbers[i % extraNumbers.Length],
                    Duration = new NpgsqlTypes.NpgsqlRange<DateTime>(DateTime.Now, DateTime.Now),
                    LocalizedName = streets[i % streets.Length],
                    Index2 = i
                }).ToList();

            var uploader = new NpgsqlBulkUploader(context);

            var sw = Stopwatch.StartNew();
            uploader.Insert(data);
            sw.Stop();

            Console.WriteLine($"Derived: dynamic solution inserted {data.Count} records for {sw.Elapsed }");
            // Trace.Assert(context.Addresses.Count() == data.Count);

            uploader.Insert(data.Take(100), InsertConflictAction.UpdateProperty<Address2EF>(
                x => x.AddressId, x => x.PostalCode));

            uploader.Insert(data.Take(100), InsertConflictAction.UpdateProperty<Address2EF>(
                x => x.AddressId, x => x.Index2));

            Console.WriteLine($"Derived: derived objects are inserted");

            var data2 = Enumerable.Range(0, 100000)
                .Select((x, i) => new Address2EF()
                {
                    StreetName = streets[i % streets.Length],
                    HouseNumber = i + 1,
                    PostalCode = codes[i % codes.Length],
                    ExtraHouseNumber = extraNumbers[i % extraNumbers.Length],
                    Duration = new NpgsqlTypes.NpgsqlRange<DateTime>(DateTime.Now, DateTime.Now),
                    LocalizedName = streets[i % streets.Length],
                    Index2 = i
                }).ToList();

            uploader.Update(data2);

        }

        static void TestInsertPartialUpdateAndIndsert(BulkContext context, List<Address> data)
        {

            Console.WriteLine("");
            Console.WriteLine("TestInsertPartialUpdateAndIndsert...");
            Console.WriteLine("");

            context.Database.ExecuteSqlRaw("TRUNCATE addresses CASCADE");

            var uploader = new NpgsqlBulkUploader(context);

            uploader.Insert(data.Take(1000));

            uploader.Update(data.Take(1000), x => x.Duration, x => x.ExtraHouseNumber);

            uploader.Insert(data.Skip(1000).Take(1000));

        }
    }
}
