using Microsoft.EntityFrameworkCore;

namespace Npgsql.Bulk.DAL
{
    public class BulkContext : DbContext
    {
        public DbSet<Address> Addresses { get; set; }

        public DbSet<Address2> Addresses2 { get; set; }

        public BulkContext(DbContextOptions<BulkContext> options) : base(options) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            //modelBuilder.Entity<Address2>().Property(x => x.AddressId).HasColumnName("base_address_id");
        }
    }
}
