using Npgsql.Bulk.DAL;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

namespace Npgsql.Bulk
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("Trying plain case...");
            TestPlainCase();
            Console.WriteLine();
            Console.WriteLine("Trying inheritance case...");
            TestInheritanceCase();
            Console.WriteLine();

            Console.WriteLine("Trying BulkSelect case...");
            TestBulkWhere();

            Console.WriteLine("Press enter to exit");
            Console.ReadLine();
        }

        static void TestPlainCase()
        {
            var streets = new[] { "First", "Second", "Third" };
            var codes = new[] { "001001", "002002", "003003", "004004" };
            var extraNumbers = new int?[] { null, 1, 2, 3, 5, 8, 13, 21, 34 };
            var addressTypes = new AddressType?[] { null, AddressType.Type1, AddressType.Type2 };
            var dates = new DateTime?[] { null, DateTime.Now };
            var guids = new Guid?[] { null, Guid.Empty };
            var decimals = new decimal?[] { null, decimal.Zero };

            var context = new BulkContext("DefaultConnection");
            context.Database.ExecuteSqlCommand("TRUNCATE addresses cascade");

            var data = Enumerable.Range(0, 100000)
                .Select((x, i) => new Address()
                {
                    StreetName = streets[i % streets.Length],
                    HouseNumber = i + 1,
                    PostalCode = codes[i % codes.Length],
                    ExtraHouseNumber = extraNumbers[i % extraNumbers.Length],
                    Type = addressTypes[i % addressTypes.Length],
                    Date = dates[i % dates.Length],
                    Guid = guids[i % guids.Length],
                    Dec = decimals[i % decimals.Length]
                }).ToList();

            var uploader = new NpgsqlBulkUploader(context);

            context.Database.ExecuteSqlCommand("DELETE FROM addresses");
            var sw = Stopwatch.StartNew();
            HardcodedInsert(data, context);
            sw.Stop();
            Console.WriteLine($"Hardcoded solution inserted {data.Count} records for {sw.Elapsed }");

            context.Database.ExecuteSqlCommand("DELETE FROM addresses");
            sw = Stopwatch.StartNew();

            uploader.Insert(data, InsertConflictAction.UpdateProperty<Address>(x => x.AddressId, x => x.Dec));

            uploader.Insert(data, InsertConflictAction.DoNothing());
            sw.Stop();
            Console.WriteLine($"Dynamic solution inserted {data.Count} records for {sw.Elapsed }");

            data.ForEach(x => x.HouseNumber += 1);

            sw = Stopwatch.StartNew();
            uploader.Update(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed }");

            context.Database.ExecuteSqlCommand("DELETE FROM addresses");
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

        static void TestInheritanceCase()
        {
            var streets = new[] { "First", "Second", "Third" };
            var codes = new[] { "001001", "002002", "003003", "004004" };
            var extraNumbers = new int?[] { null, 1, 2, 3, 5, 8, 13, 21, 34 };

            var context = new BulkContext("DefaultConnection");
            context.Database.ExecuteSqlCommand("DELETE FROM addresses2");

            var data = Enumerable.Range(0, 100000)
                .Select((x, i) => new Address2()
                {
                    StreetName = streets[i % streets.Length],
                    HouseNumber = i + 1,
                    PostalCode = codes[i % codes.Length],
                    ExtraHouseNumber = extraNumbers[i % extraNumbers.Length],
                    LocalizedName = streets[i % streets.Length],
                    Index1 = i,
                    Index2 = i
                }).ToList();

            var uploader = new NpgsqlBulkUploader(context);

            context.Database.ExecuteSqlCommand("TRUNCATE addresses CASCADE");
            var sw = Stopwatch.StartNew();
            uploader.Insert(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution inserted {data.Count} records for {sw.Elapsed }");

            // checking consitency
            foreach (var addr in data)
            {
                if (addr.Index1 != addr.Index2)
                {
                    Console.WriteLine($"INCONSITENT! Id: {addr.AddressId}/{addr.Address2Id}, {addr.Index1} != {addr.Index2}");
                }
            }

            data.ForEach(x => x.HouseNumber += 1);

            sw = Stopwatch.StartNew();
            uploader.Update(data);
            sw.Stop();
            Console.WriteLine($"Dynamic solution updated {data.Count} records for {sw.Elapsed }");
        }

        static void TestBulkWhere()
        {
            var context = new BulkContext("DefaultConnection");

            var baseQuery = context.Addresses2
                .Select(x => new { StreetName = x.StreetName, x.HouseNumber, x.CreatedAt });
            var queries = baseQuery.Take(50000).Select(x => new { StreetName = x.StreetName, x.HouseNumber }).ToList();

            // Filter by unique subcollections
            var sw = Stopwatch.StartNew();
            var uniqueStreetname = queries.Select(x => x.StreetName).Distinct().ToList();
            var uniqueHouseNumbers = queries.Select(x => x.HouseNumber).Distinct().ToList();
            var result = baseQuery.Where(x => uniqueStreetname.Contains(x.StreetName) &&
                uniqueHouseNumbers.Contains(x.HouseNumber)).ToList();

            result = (from r in result
                      join q in queries on new { r.StreetName, r.HouseNumber } equals new { q.StreetName, q.HouseNumber }
                      select r).ToList();

            sw.Stop();
            Console.WriteLine($"Filter by unique collections method extracted {result.Count} records for {sw.Elapsed }");
            Console.WriteLine();

            // Union approach
            sw = Stopwatch.StartNew();
            var currentQuery = queries[0];
            var query = baseQuery.Where(x => x.StreetName == currentQuery.StreetName &&
                x.HouseNumber == currentQuery.HouseNumber);
            for (var i = 1; i < 100; i++)
            {
                var cq = queries[i];
                query = query.Union(baseQuery.Where(x => x.StreetName == cq.StreetName &&
                    x.HouseNumber == cq.HouseNumber));
            }
            result = query.ToList();
            sw.Stop();
            Console.WriteLine($"Union extracted {result.Count} records for {sw.Elapsed }");
            Console.WriteLine();

            // Bulk Select approach
            sw = Stopwatch.StartNew();
            result = baseQuery.BulkSelect(x => new { x.StreetName, x.HouseNumber }, queries);
            sw.Stop();
            Console.WriteLine($"BulkSelect extracted {result.Count} records for {sw.Elapsed }");
            Console.WriteLine();

            // Bulk Select (Contains) approach
            sw = Stopwatch.StartNew();
            result = baseQuery.BulkSelect(x => new { x.HouseNumber },
                queries.Select(x => new { x.HouseNumber }).Distinct().ToList());
            sw.Stop();
            Console.WriteLine($"BulkSelect (Contains) extracted {result.Count} records for {sw.Elapsed }");
            
            Console.WriteLine();
        }

        class BulkSelect
        {
            public string StreetName { get; set; }

            public int NumberFrom { get; set; }

            public int NumberTo { get; set; }
        }

        static void HardcodedInsert(List<Address> addresses, DbContext context)
        {
            var db = context.Database;
            var conn = (NpgsqlConnection)db.Connection;

            if (conn.State != System.Data.ConnectionState.Open)
                conn.Open();

            var transaction = db.BeginTransaction();

            try
            {
                // 0. Prepare variables
                var dataColumns = "street_name, house_number, extra_house_number, postal_code";
                var dbGenColumns = "address_id, created_at";

                // 1. Create temp table 
                db.ExecuteSqlCommand($"CREATE TEMP TABLE addresses_temp_input ON COMMIT DROP AS SELECT {dataColumns} FROM addresses LIMIT 0");

                // 2. Import into temp table
                using (var importer = conn.BeginBinaryImport($"COPY addresses_temp_input ({dataColumns}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var address in addresses)
                    {
                        importer.StartRow();
                        importer.Write(address.StreetName);
                        importer.Write(address.HouseNumber);
                        if (address.ExtraHouseNumber.HasValue)
                            importer.Write(address.ExtraHouseNumber.Value);
                        else
                            importer.WriteNull();

                        importer.Write(address.PostalCode);
                    }
                    importer.Complete();
                }

                // 3. Insert into real table from temp one
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO addresses ({dataColumns}) SELECT {dataColumns} FROM addresses_temp_input RETURNING {dbGenColumns}";
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();

                        // 4. Propagate computed value
                        for (var i = 0; i < addresses.Count; i++)
                        {
                            addresses[i].AddressId = (int)reader["address_id"];
                            addresses[i].CreatedAt = ((DateTimeOffset)reader["created_at"]).DateTime;
                        }
                    }
                }

                // 5. Commit
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }
}
