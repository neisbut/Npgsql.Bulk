using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Npgsql.Bulk.DAL;
using Npgsql.Bulk.SampleRunner.DotNetStandard20;
using Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL;

namespace Npgsql.Bulk.IntegrationTests.DotNetStandard20
{
    internal static class ContextExtensions
    {
        public static BulkContext CreateContext()
        {
            return new BulkContextFactory()
                .CreateDbContext(new string[0]);
        }

        public static async Task AddToDb(Address address)
        {
            var addContext = CreateContext();
            addContext.Add(address);
            await addContext.SaveChangesAsync();
        }

        public static async Task DeleteIfExists(int addressId)
        {
            var deleteContext = ContextExtensions.CreateContext();
            var foundAddress = await deleteContext.Addresses
                .SingleOrDefaultAsync(x => x.AddressId == addressId);

            if (foundAddress != null)
            {
                deleteContext.Remove(foundAddress);
                await deleteContext.SaveChangesAsync();
            }
        }
    
        public static async Task DeleteIfExistsByName(string name)
        {
            var deleteContext = CreateContext();
            var foundAddresses = await deleteContext.Addresses
                .Where(x => x.StreetName == name)
                .ToArrayAsync();

            deleteContext.Addresses.RemoveRange(foundAddresses);
            await deleteContext.SaveChangesAsync();
        }
    }
}