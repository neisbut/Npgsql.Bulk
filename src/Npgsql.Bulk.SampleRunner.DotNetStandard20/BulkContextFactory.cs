using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Npgsql.Bulk.DAL;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20
{
    public class BulkContextFactory : IDesignTimeDbContextFactory<BulkContext>
    {
        public BulkContext CreateDbContext(string[] args)
        {
            var options = new DbContextOptionsBuilder<BulkContext>()
                .UseNpgsql(Configuration.ConnectionString)
                .Options;

            return new BulkContext(options);
        }
    }
}