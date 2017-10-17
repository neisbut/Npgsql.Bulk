using Microsoft.EntityFrameworkCore;

namespace Npgsql.Bulk.DAL
{
    public class BulkContext : DbContext
    {
        public DbSet<Address> Addresses { get; set; }

        public BulkContext(DbContextOptions<BulkContext> options) : base(options) { }
    }
}
