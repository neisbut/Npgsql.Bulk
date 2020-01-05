using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Npgsql.Bulk.Model;

namespace Npgsql.Bulk
{
    class RelationalHelper : IRelationalHelper
    {
        public DbContextTransaction EnsureOrStartTransaction(
            DbContext context, IsolationLevel isolation)
        {
            if (context.Database.CurrentTransaction == null)
            {
                if (System.Transactions.Transaction.Current != null)
                {
                    //System.Transactions.TransactionsDatabaseFacadeExtensions.EnlistTransaction(context.Database, System.Transactions.Transaction.Current);
                    return null;
                }

                return context.Database.BeginTransaction(isolation);
            }

            return null;
        }


        public List<ColumnInfo> GetColumnsInfo(DbContext context, string tableName)
        {
            var sql = @"
                    SELECT column_name as ColumnName, udt_name as ColumnType, data_type as ColumnTypeExtra, (column_default IS NOT NULL) as HasDefault 
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                    ORDER BY ordinal_position";
            var param = new NpgsqlParameter("@tableName", tableName);

            return context.Database.SqlQuery<ColumnInfo>(sql, param).ToList();
        }

        public List<MappingInfo> GetMetadata(DbContext context, Type type)
        {
            var metadata = ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;
            var objectItemCollection = ((ObjectItemCollection)metadata.GetItemCollection(DataSpace.OSpace));

            // Get the entity type from the model that maps to the CLR type
            var entityType = metadata.GetItems<EntityType>(DataSpace.OSpace)
                .Single(e => objectItemCollection.GetClrType(e) == type);

            var sets = metadata.GetItems<EntityContainer>(DataSpace.CSpace).Single().EntitySets;
            var entitySet = sets.SingleOrDefault(s => s.ElementType.Name == entityType.Name);

            var mappings = metadata.GetItems<EntityContainerMapping>(DataSpace.CSSpace).Single().EntitySetMappings;

            if (entitySet != null)
            {
                var mapping = mappings.Single(s => s.EntitySet == entitySet);
                var typeMappings = mapping.EntityTypeMappings;
                var mappingFragment = (typeMappings.Count == 1 ?
                    typeMappings.Single() :
                    typeMappings.Single(x => x.EntityType == null)).Fragments.Single();

                return ConvertFragmentToMapping(context, type, mappingFragment, entityType);
            }
            else
            {
                var partMapping = mappings.SelectMany(x => x.EntityTypeMappings)
                    .Where(x => x.EntityType != null)
                    .Where(x => x.EntityType.Name == type.Name).FirstOrDefault();

                if (partMapping.EntityType.BaseType != null)
                {
                    var baseEntityType = metadata.GetItems<EntityType>(DataSpace.OSpace)
                        .Single(e => e.Name == partMapping.EntityType.BaseType.Name);
                    var baseClrType = objectItemCollection.GetClrType(baseEntityType);

                    var baseTypeMapping = GetMetadata(context, baseClrType);
                    var subTypeMapping = ConvertFragmentToMapping(
                        context, type, partMapping.Fragments.Single(), entityType);

                    var union = baseTypeMapping.Union(subTypeMapping).ToList();
                    union.ForEach(x => x.IsInheritanceUsed = true);
                    return union;
                }
                else
                {
                    throw new NotSupportedException();
                }

            }
        }

        public NpgsqlConnection GetNpgsqlConnection(DbContext context)
        {
            return (NpgsqlConnection)context.Database.Connection;
        }

        internal List<MappingInfo> ConvertFragmentToMapping(
            DbContext context,
            Type type,
            MappingFragment mappingFragment,
            EntityType entityType)
        {
            var tableEntitySet = mappingFragment.StoreEntitySet;
            var tableName = (tableEntitySet.MetadataProperties["Table"].Value ?? tableEntitySet.Name).ToString();

            var columnsInfo = GetColumnsInfo(context, tableName);
            var innerList = mappingFragment.PropertyMappings
                    .OfType<ScalarPropertyMapping>()
                    .Select(x =>
                    {
                        var columnInfo = columnsInfo.FirstOrDefault(c => c.ColumnName == x.Column.Name);
                        if (columnInfo == null)
                            throw new InvalidOperationException(
                                $"Column '{x.Column.Name}' (mapped to: '{x.Property.DeclaringType.Name}.{x.Property.Name}') is not found");
                        var isDbGenerated = x.Column.IsStoreGeneratedComputed || x.Column.IsStoreGeneratedIdentity;

                        return new MappingInfo()
                        {
                            TableName = tableName,
                            TableNameQualified = NpgsqlHelper.GetQualifiedName(tableName, tableEntitySet.Schema),
                            Property = type.GetProperty(x.Property.Name,
                                BindingFlags.NonPublic | BindingFlags.Public |
                                BindingFlags.GetProperty | BindingFlags.Instance),
                            ColumnInfo = columnInfo,
                            IsKey = entityType.KeyProperties.Any(y => y.Name == x.Property.Name),
                            DoUpdate = !isDbGenerated,
                            DoInsert = !isDbGenerated
                        };
                    }).ToList();

            return innerList;
        }
    }
}
