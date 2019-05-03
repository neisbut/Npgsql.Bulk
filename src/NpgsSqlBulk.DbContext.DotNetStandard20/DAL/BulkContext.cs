using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ValueGeneration;

namespace NpgsSqlBulk.DbContext.DotNetStandard20.DAL
{
    public class BulkContext : Microsoft.EntityFrameworkCore.DbContext
    {
        public DbSet<Address> Addresses { get; set; }


        public BulkContext(DbContextOptions<BulkContext> options) : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<Address>().Property(x => x.CreatedAt)
                .HasValueGenerator<ValueGen>().ValueGeneratedOnAdd();
        }
    }

    public class ValueGen : ValueGenerator
    {
        public override bool GeneratesTemporaryValues => false;

        protected override object NextValue(EntityEntry entry)
        {
            return DateTime.Now;
        }
    }
}