using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql.Bulk.Model;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace Npgsql.Bulk
{
    /// <summary>
    /// Some internal helper methods for Npgsql
    /// </summary>
    internal static class NpgsqlHelper
    {
        internal static string GetQualifiedName(string name, string prefix = null)
        {
            return $"{(prefix == null ? "" : "\"" + prefix + "\".")}\"{name}\"";
        }

        internal static List<MappingInfo> GetMetadata(DbContext context, Type type)
        {
            var metadata = context.Model;
            var entityType = metadata.GetEntityTypes().Single(x => x.ClrType == type);

            var tableName = GetTableName(context, type);
            var columnsInfo = NpgsqlBulkUploader.RelationalHelper.GetColumnsInfo(context, tableName);

            if (entityType.BaseType != null)
            {
                var baseTableName = GetTableName(context, entityType.BaseType.ClrType);
                if (baseTableName != tableName)
                {
                    var extraColumnsInfo = NpgsqlBulkUploader.RelationalHelper.GetColumnsInfo(context, baseTableName);
                    columnsInfo.AddRange(extraColumnsInfo);
                }
            }

            var valueGenSelector = ((IInfrastructure<IServiceProvider>)context).Instance.GetService<IValueGeneratorSelector>();

            int optinalIndex = 0;
            var innerList = entityType
                .GetProperties()
                .Where(x => x.GetColumnName() != "xmin") // For now we don't support xmin
                .Select(x =>
                {
                    var relational = x.DeclaringEntityType;
                    ValueGenerator localGenerator = null;
                    bool isDbGenerated = false;

                    var generatorFactory = x.GetAnnotations().FirstOrDefault(a => a.Name == "ValueGeneratorFactory");
                    if (generatorFactory != null)
                    {
                        var valueGeneratorAccessor = generatorFactory.Value as Func<IProperty, IEntityType, ValueGenerator>;
                        localGenerator = valueGeneratorAccessor(x, x.DeclaringEntityType);
                    }
                    else if (x.GetAnnotations().Any(y => y.Name == "Relational:ComputedColumnSql"))
                    {
                        isDbGenerated = true;
                    }
                    else
                    {
                        var autoGenStrategy = x.GetAnnotations().FirstOrDefault(y => y.Name == "Npgsql:ValueGenerationStrategy");
                        if (autoGenStrategy != null)
                        {
                            var npgsqlStartegy = (NpgsqlValueGenerationStrategy)autoGenStrategy.Value;
                            isDbGenerated = npgsqlStartegy != NpgsqlValueGenerationStrategy.SequenceHiLo &&
                                npgsqlStartegy != NpgsqlValueGenerationStrategy.None;
                        }
                    }

                    if (!isDbGenerated && localGenerator == null)
                    {
                        try
                        {
                            localGenerator = valueGenSelector.Select(x, entityType);
                        }
                        catch
                        {
                            // ignore
                        }
                    }

#if DotNet6 || DotNet7
                    var readBack = x.GetStoreGeneratedIndex() >= 0;
#else
                    var indexes = ((Microsoft.EntityFrameworkCore.Metadata.Internal.Property)x).PropertyIndexes;
                    var readBack = indexes.StoreGenerationIndex >= 0;
#endif

                    long optionalFlag = 0;

                    // We don't support genertion based on Foreign Keys. 
                    if (readBack && !x.IsForeignKey() && localGenerator == null)
                    {
                        optionalFlag = 1 << optinalIndex;
                        optinalIndex++;
                    }

                    return new MappingInfo()
                    {
                        TableName = relational.GetTableName(),
                        TableNameQualified = NpgsqlHelper.GetQualifiedName(relational.GetTableName(), relational.GetSchema()),
                        Property = x.PropertyInfo,
                        ColumnInfo = columnsInfo.First(c => c.ColumnName == x.GetColumnName()),
                        LocalGenerator = localGenerator,
                        ValueConverter = x.GetValueConverter(),
                        IsKey = x.IsKey(),
                        IsInheritanceUsed = entityType.BaseType != null,
                        DbProperty = x,
                        DoUpdate = !isDbGenerated && x.PropertyInfo != null,    // don't update shadow props
                        DoInsert = !isDbGenerated,                              // do insert of shadow props
                        ReadBack = readBack,
                        IsSpecifiedFlag = optionalFlag
                    };
                }).ToList();

            return innerList;
        }

        internal static string GetTableName(DbContext context, Type t)
        {
            var relational = context.Model.FindEntityType(t);
            return relational.GetTableName();
        }

        internal static string GetTableSchema(DbContext context, Type t)
        {
            var relational = context.Model.FindEntityType(t);
            return relational.GetSchema();
        }

        internal static string GetTableNameQualified(DbContext context, Type t)
        {
            var relational = context.Model.FindEntityType(t);
            return GetQualifiedName(relational.GetTableName(), relational.GetSchema());
        }

    }
}
