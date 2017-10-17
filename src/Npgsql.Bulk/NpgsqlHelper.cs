using Npgsql.Bulk.Model;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            return (NpgsqlConnection)context.Database.Connection;
        }

        internal static DbContextTransaction EnsureOrStartTransaction(DbContext context)
        {
            if (context.Database.CurrentTransaction == null)
                return context.Database.BeginTransaction();
            return null;
        }

        internal static List<ColumnInfo> GetColumnsInfo(DbContext context, string tableName)
        {
            var sql = @"
                    SELECT column_name as ColumnName, udt_name as ColumnType, data_type as ColumnTypeExtra, (column_default IS NOT NULL) as HasDefault 
                    FROM information_schema.columns
                    WHERE table_name = @tableName";
            var param = new NpgsqlParameter("@tableName", tableName);

            return context.Database.SqlQuery<ColumnInfo>(sql, param).ToList();
        }
    }
}
