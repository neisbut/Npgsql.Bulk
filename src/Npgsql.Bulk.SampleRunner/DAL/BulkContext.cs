using System.Data.Entity;

namespace Npgsql.Bulk.DAL
{
    public class BulkContext : DbContext
    {
        public DbSet<Address> Addresses { get; set; }

        public DbSet<Address2> Addresses2 { get; set; }

        public BulkContext(string csName) : base(csName) { }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<Address2>().HasEntitySetName("Address2");
            modelBuilder.Entity<Address2>().Property(x => x.AddressId).HasColumnName("base_address_id");
        }
    }
}
