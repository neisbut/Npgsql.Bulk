using Npgsql.Bulk.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.ModelConfiguration.Configuration;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk
{
    /// <summary>
    /// Some internal helper methods for Npgsql
    /// </summary>
    internal static class NpgsqlHelper
    {
        internal static DbContext GetContextFromQuery(IQueryable query)
        {
            var intQueryProp = query.GetType().GetProperty(
                "InternalQuery", BindingFlags.Instance | BindingFlags.NonPublic);
            var intQuery = intQueryProp.GetValue(query);
            var objQueryProp = intQuery.GetType().GetProperty(
                "ObjectQuery", BindingFlags.Instance | BindingFlags.Public);
            var objQuery = (ObjectQuery)objQueryProp.GetValue(intQuery);

            return objQuery.Context.InterceptionContext.DbContexts.First();
        }

        internal static string GetQualifiedName(string name, string prefix = null)
        {
            return $"{(prefix == null ? "" : "\"" + prefix + "\".")}\"{name}\"";
        }

        internal static NpgsqlConnection GetNpgsqlConnection(DbContext context)
        {
            return (NpgsqlConnection)context.Database.Connection;
        }

        internal static DbContextTransaction EnsureOrStartTransaction(
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

        internal static List<ColumnInfo> GetColumnsInfo(DbContext context, string tableName)
        {
            var sql = @"
                    SELECT column_name as ColumnName, udt_name as ColumnType, data_type as ColumnTypeExtra, (column_default IS NOT NULL) as HasDefault 
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                    ORDER BY ordinal_position";
            var param = new NpgsqlParameter("@tableName", tableName);

            return context.Database.SqlQuery<ColumnInfo>(sql, param).ToList();
        }

        internal static List<MappingInfo> ConvertFragmentToMapping(
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

                        return new MappingInfo()
                        {
                            TableName = tableName,
                            TableNameQualified = GetQualifiedName(tableName, tableEntitySet.Schema),
                            Property = type.GetProperty(x.Property.Name,
                                BindingFlags.NonPublic | BindingFlags.Public |
                                BindingFlags.GetProperty | BindingFlags.Instance),
                            ColumnInfo = columnInfo,
                            IsDbGenerated = x.Column.IsStoreGeneratedComputed || x.Column.IsStoreGeneratedIdentity,
                            IsKey = entityType.KeyProperties.Any(y => y.Name == x.Property.Name)
                        };
                    }).ToList();

            return innerList;
        }

        internal static EntitySetBase GetEntitySet(DbContext context, Type type)
        {
            var metadata = ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;
            string baseTypeName = type.BaseType.Name;
            string typeName = type.Name;

            var es = metadata
                    .GetItemCollection(DataSpace.SSpace)
                    .GetItems<EntityContainer>()
                    .SelectMany(c => c.BaseEntitySets
                        .Where(e => e.Name == typeName || e.Name == baseTypeName))
                    .FirstOrDefault();

            return es;
        }

        internal static List<MappingInfo> GetMetadata(DbContext context, Type type)
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

        internal static string GetTableName(DbContext context, Type t)
        {
            var entityType = GetEntitySet(context, t);
            return entityType.Table;
        }

        internal static string GetTableNameQualified(DbContext context, Type t)
        {
            var entityType = GetEntitySet(context, t);
            return GetQualifiedName(entityType.Table, entityType.Schema);
        }
    }
}
