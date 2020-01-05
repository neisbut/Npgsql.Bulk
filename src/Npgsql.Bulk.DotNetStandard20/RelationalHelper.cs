using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Npgsql.Bulk.Model;

namespace Npgsql.Bulk
{
    class RelationalHelper : IRelationalHelper
    {
        public IDbContextTransaction EnsureOrStartTransaction(
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

        public NpgsqlConnection GetNpgsqlConnection(DbContext context)
        {
            return (NpgsqlConnection)context.Database.GetDbConnection();
        }


    }
}
