using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using NpgsSqlBulk.DbContext.DotNetStandard20.DAL;

namespace NpgsSqlBulk.DbContext.DotNetStandard20
{
    public class BulkContextFactory : IDesignTimeDbContextFactory<BulkContext>
    {
        public BulkContext CreateDbContext(string[] args)
        {
            var options = new DbContextOptionsBuilder<BulkContext>()
                .UseNpgsql("server=localhost;user id=postgres;password=qwerty;database=copy;port=5432")
                .Options;

            return new BulkContext(options);
        }
    }
}