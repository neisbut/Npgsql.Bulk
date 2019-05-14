using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using NpgsSqlBulk.DbContext.DotNetStandard20;
using NpgsSqlBulk.DbContext.DotNetStandard20.DAL;
using Xunit;

namespace Npgsql.Bulk.IntegrationTests.DotNetStandard20
{
    public class BulkUpdateTests
    {
        [Fact]
        public async Task Update_RecordsAreUpdated()
        {
            const string initialStreetName = "Initial Street";
            const string finalStreetName = "Final Street";

            const string initialPostalCode = "Initial Postal Code";
            const string finalPostalCode = "Final Postal Code";

            const int addressId = 200000;

            await DeleteAddressIfExists(addressId);

            var address = new Address
            {
                AddressId = addressId,
                StreetName = initialStreetName,
                HouseNumber = 320,
                PostalCode = initialPostalCode,
                ExtraHouseNumber = 1456,
                Duration = new NpgsqlTypes.NpgsqlRange<DateTime>(DateTime.Now, DateTime.Now)
            };

            await AddEntityToContext(address);

            address.StreetName = finalStreetName;
            address.PostalCode = finalPostalCode;


            var bulkImport = new NpgsqlBulkUploader(CreateContext());
            bulkImport.Update(new[] {address});


            var actualAddress = await CreateContext().Addresses.SingleAsync(x => x.AddressId == addressId);
            actualAddress.StreetName.Should().BeEquivalentTo(finalStreetName);
            actualAddress.PostalCode.Should().BeEquivalentTo(finalPostalCode);
        }

        private BulkContext CreateContext()
        {
            return new BulkContextFactory()
                .CreateDbContext(new string[0]);
        }

        private async Task AddEntityToContext(Address address)
        {
            var addContext = CreateContext();
            addContext.Add(address);
            await addContext.SaveChangesAsync();
        }

        private async Task DeleteAddressIfExists(int addressId)
        {
            var deleteContext = CreateContext();
            var foundAddress = await deleteContext.Addresses
                .SingleOrDefaultAsync(x => x.AddressId == addressId);

            if (foundAddress != null)
            {
                deleteContext.Remove(foundAddress);
                await deleteContext.SaveChangesAsync();
            }
        }
    }
}