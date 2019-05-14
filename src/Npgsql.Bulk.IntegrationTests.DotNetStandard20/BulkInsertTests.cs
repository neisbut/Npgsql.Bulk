using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL;
using Xunit;
using static Npgsql.Bulk.IntegrationTests.DotNetStandard20.ContextExtensions;

namespace Npgsql.Bulk.IntegrationTests.DotNetStandard20
{
    public class BulkInsertTests
    {
        [Fact]
        public async Task Insert_RecordIsInserted()
        {
            const string streetName = "Just inserted street name";

            await DeleteIfExistsByName(streetName);

            var address = new Address
            {
                StreetName = streetName,
                HouseNumber = 320,
                PostalCode = "Some postal code",
                ExtraHouseNumber = 1456,
                Duration = new NpgsqlTypes.NpgsqlRange<DateTime>(DateTime.Now, DateTime.Now)
            };
            
            var bulkImport = new NpgsqlBulkUploader(CreateContext());
            bulkImport.Insert(new[] {address});


            var assertContext = CreateContext();
            var actualAddress = await assertContext.Addresses
                .SingleAsync(x => x.StreetName == streetName);

            using (assertContext)
            {
                actualAddress.StreetName.Should().Be(address.StreetName);
                actualAddress.HouseNumber.Should().Be(address.HouseNumber);
                actualAddress.PostalCode.Should().Be(address.PostalCode);
                actualAddress.ExtraHouseNumber.Should().Be(address.ExtraHouseNumber);
                actualAddress.Duration.ToString().Should().Be(address.Duration.ToString());
            }
        }
    }
}