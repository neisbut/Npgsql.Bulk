using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql.Bulk.Model;
using System;
using System.Collections.Generic;
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
            using(var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.Parameters.Add(param);
                var result = new List<ColumnInfo>();
                using(var reader = cmd.ExecuteReader())
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
    }
}
