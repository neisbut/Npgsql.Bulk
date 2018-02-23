using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql.Bulk.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Reflection;

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

        internal static NpgsqlConnection GetNpgsqlConnection(DbContext context)
        {
            return (NpgsqlConnection)context.Database.GetDbConnection();
        }

        internal static IDbContextTransaction EnsureOrStartTransaction(DbContext context)
        {
            if (context.Database.CurrentTransaction == null)
                return context.Database.BeginTransaction();
            return null;
        }

        internal static List<ColumnInfo> GetColumnsInfo(DbContext context, string tableName)
        {
            var sql = @"
                    SELECT column_name as ColumnName, udt_name as ColumnType, (column_default IS NOT NULL) as HasDefault 
                    FROM information_schema.columns
                    WHERE table_name = @tableName";
            var param = new NpgsqlParameter("@tableName", tableName);

            var conn = GetNpgsqlConnection(context);
            if (conn.State != ConnectionState.Open)
                conn.Open();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.Parameters.Add(param);
                var result = new List<ColumnInfo>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(new ColumnInfo()
                        {
                            ColumnName = (string)reader["ColumnName"],
                            ColumnType = (string)reader["ColumnType"],
                            HasDefault = (bool)reader["HasDefault"]
                        });
                    }
                }
                return result;
            }
        }

        internal static List<MappingInfo> GetMetadata(DbContext context, Type type)
        {
            var metadata = context.Model;
            var entityType = metadata.GetEntityTypes().Single(x => x.ClrType == type);

            var tableName = GetTableName(context, type);
            var columnsInfo = GetColumnsInfo(context, tableName);
            if (entityType.BaseType != null)
            {
                var baseTableName = GetTableName(context, entityType.BaseType.ClrType);
                if (baseTableName != tableName)
                {
                    var extraColumnsInfo = GetColumnsInfo(context, baseTableName);
                    columnsInfo.AddRange(extraColumnsInfo);
                }
            }

            var innerList = entityType.GetProperties()
                .Where(x => x.PropertyInfo != null)
                .Select(x =>
                {
                    var relational = x.DeclaringEntityType.Relational();
                    return new MappingInfo()
                    {
                        TableName = relational.TableName,
                        TableNameQualified = NpgsqlHelper.GetQualifiedName(relational.TableName, relational.Schema),
                        Property = x.PropertyInfo,
                        ColumnInfo = columnsInfo.First(c => c.ColumnName == x.Relational().ColumnName),
                        IsDbGenerated = x.ValueGenerated != ValueGenerated.Never,
                        IsKey = x.IsKey(),
                        IsInheritanceUsed = entityType.BaseType != null
                    };
                }).ToList();

            return innerList;
        }

        internal static string GetTableName(DbContext context, Type t)
        {
            var relational = context.Model.FindEntityType(t).Relational();
            return relational.TableName;
        }

        internal static string GetTableNameQualified(DbContext context, Type t)
        {
            var relational = context.Model.FindEntityType(t).Relational();
            return GetQualifiedName(relational.TableName, relational.Schema);
        }
    }
}
