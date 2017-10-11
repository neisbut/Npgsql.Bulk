using System.Data.Entity;

namespace Npgsql.Bulk.DAL
{
    public class BulkContext : DbContext
    {
        public DbSet<Address> Addresses { get; set; }

        public BulkContext(string csName) : base(csName) { }
    }
}
