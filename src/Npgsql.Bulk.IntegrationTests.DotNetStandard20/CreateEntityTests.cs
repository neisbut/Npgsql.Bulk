using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql.Bulk.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Text;
using Xunit;

namespace Npgsql.Bulk.IntegrationTests.DotNetStandard20
{
    public class CreateEntityTests
    {
        [Fact]
        public void CreateEntityInfoTest1()
        {


            NpgsqlBulkUploader.RelationalHelper = new TestRelationHelper(new Dictionary<Type, List<ColumnInfo>>()
            {
                { typeof(Entity1), new List<ColumnInfo>()
                    {
                        new ColumnInfo()
                        {
                            ColumnName = nameof(Entity1.Id),
                            ColumnType = "uuid"
                        },
                        new ColumnInfo()
                        {
                            ColumnName = nameof(Entity1.Number),
                            ColumnType = "int"
                        },
                        new ColumnInfo()
                        {
                            ColumnName = nameof(Entity1.Value),
                            ColumnType = "text"
                        }
                    }
                }
            });

            var ctx1 = new TestContext<Tuple<int>>((builder) =>
            {
                builder.Entity<Entity1>().HasKey(x => x.Id);
                builder.Entity<Entity1>().Property(x => x.Id).HasComputedColumnSql("SQL");
            });
            var uploader = new NpgsqlBulkUploader(ctx1);
            var info = uploader.CreateEntityInfo<Entity1>();

            // When HasComputedColumnSql used -> should not have Id in Insert and Update
            Assert.DoesNotContain(info.InsertClientDataInfos, x => x.Property.Name == "Id");

            // Key is needed for Update
            Assert.Contains(info.UpdateClientDataWithKeysInfos, x => x.Property.Name == "Id");

            var ctx2 = new TestContext<Tuple<long>>((builder) =>
            {
                builder.Entity<Entity1>().HasKey(x => x.Id);
            });
            uploader = new NpgsqlBulkUploader(ctx2);
            info = uploader.CreateEntityInfo<Entity1>();

            // When No HasComputedColumnSql used -> should have Id in Insert and Update
            Assert.Contains(info.InsertClientDataInfos, x => x.Property.Name == "Id");
            Assert.Contains(info.UpdateClientDataWithKeysInfos, x => x.Property.Name == "Id");
        }

        [Fact]
        public void GetValueTest()
        {
            var ctx = new TestContext<long>(b => { });
            var metadata = ctx.Model;
            var entityType = metadata.GetEntityTypes().Single(x => x.ClrType == typeof(Entity1));

            ValueHelper<Entity1>.MappingInfos = new Dictionary<string, MappingInfo>()
            {
                {
                    "Number",
                    new MappingInfo() {
                        DbProperty = entityType.GetProperties().First(x=>x.Name =="Number"),
                    }
                },
                {
                    "Id",
                    new MappingInfo() {
                        DbProperty = entityType.GetProperties().First(x=>x.Name =="Id"),
                    }
                }
            };

            // test default value
            var entity = new Entity1();
            var valueInt = ValueHelper<Entity1>.Get<int, int>(entity, "Number", ctx, entity.Number);
            Assert.Equal(123, valueInt);
            var valueGuid = ValueHelper<Entity1>.Get<Guid, Guid>(entity, "Id", ctx, entity.Id);
            Assert.NotEqual(Guid.Empty, valueGuid);

            // value is specified
            entity = new Entity1() { Number = 321, Id = Guid.NewGuid() };
            valueInt = ValueHelper<Entity1>.Get<int, int>(entity, "Number", ctx, entity.Number);
            Assert.Equal(321, valueInt);
            valueGuid = ValueHelper<Entity1>.Get<Guid, Guid>(entity, "Id", ctx, entity.Id);
            Assert.Equal(entity.Id, valueGuid);
        }

        private class Entity1
        {
            public Guid Id { get; set; }

            public int Number { get; set; }

            public string Value { get; set; }
        }



        /// <summary>
        /// Note: generic parameter is needed to workaround EF cache for contet type
        /// </summary>
        /// <typeparam name="TStub"></typeparam>
        private class TestContext<TStub> : DbContext
        {
            Action<ModelBuilder> builderAction;

            public DbSet<Entity1> Entities1 { get; set; }

            public TestContext(Action<ModelBuilder> builderAction)
            {
                this.builderAction = builderAction;
            }

            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                optionsBuilder.UseInMemoryDatabase("TestDb");
            }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                base.OnModelCreating(modelBuilder);
                builderAction(modelBuilder);
            }
        }

        private class TestRelationHelper : IRelationalHelper
        {

            Dictionary<string, List<ColumnInfo>> columnsInfo;

            public TestRelationHelper(Dictionary<Type, List<ColumnInfo>> columnsInfo)
            {
                this.columnsInfo = columnsInfo.ToDictionary(x => x.Key.Name, x => x.Value);
            }

            public List<ColumnInfo> GetColumnsInfo(DbContext context, string tableName)
            {
                return columnsInfo[tableName];
            }

            public List<MappingInfo> GetMetadata(DbContext context, Type type) => throw new NotImplementedException();

            public NpgsqlConnection GetNpgsqlConnection(DbContext context) => throw new NotImplementedException();

            public IDbContextTransaction EnsureOrStartTransaction(DbContext context, IsolationLevel defaultIsolationLevel) => throw new NotImplementedException();
        }
    }
}
