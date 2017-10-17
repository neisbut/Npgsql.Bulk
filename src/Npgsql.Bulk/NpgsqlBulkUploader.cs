using NpgsqlTypes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
#if NETSTANDARD1_5 || NETSTANDARD2_0
using Microsoft.EntityFrameworkCore;
#else
using System.Data.Entity;
#endif
using System.Linq;
using System.Reflection;
using Npgsql.Bulk.Model;

namespace Npgsql.Bulk
{
    /// <summary>
    /// Uploader class itself
    /// </summary>
    public class NpgsqlBulkUploader
    {
        private static readonly ConcurrentDictionary<Type, EntityInfo> Cache = new ConcurrentDictionary<Type, EntityInfo>();

        private readonly DbContext context;

        public NpgsqlBulkUploader(DbContext context)
        {
            this.context = context;
        }

        private NpgsqlDbType GetNpgsqlType(ColumnInfo info)
        {
            switch (info.ColumnType)
            {
                case "integer":
                case "int":
                case "int4":
                    return NpgsqlDbType.Integer;
                case "bool":
                    return NpgsqlDbType.Boolean;
                case "varchar":
                    return NpgsqlDbType.Varchar;
                case "char":
                    return NpgsqlDbType.Char;
                case "real":
                case "float4":
                    return NpgsqlDbType.Real;
                case "float8":
                    return NpgsqlDbType.Double;
                case "numeric":
                case "decimal":
                    return NpgsqlDbType.Numeric;
                case "text":
                    return NpgsqlDbType.Text;
                case "int8":
                case "bigint":
                    return NpgsqlDbType.Bigint;
                case "timetz":
                    return NpgsqlDbType.TimeTZ;
                case "time":
                    return NpgsqlDbType.Time;
                case "date":
                    return NpgsqlDbType.Date;
                case "smallint":
                    return NpgsqlDbType.Smallint;
                case "uuid":
                    return NpgsqlDbType.Uuid;
                case "timestamp":
                    return NpgsqlDbType.Timestamp;
                case "timestamptz":
                    return NpgsqlDbType.TimestampTZ;
                default:

                    if (info.ColumnTypeExtra.Equals("array", StringComparison.OrdinalIgnoreCase))
                        return NpgsqlDbType.Array;

                    throw new NotImplementedException();
            }
        }

        // This method may be used instead of NpgsqlBulkCodeBuilder
        private void WriteRow<T>(T item, MappingInfo[] mapInfos, NpgsqlBinaryImporter importer)
        {
            foreach (var mapItem in mapInfos)
            {
                var value = mapItem.Property.GetValue(item);
                importer.Write(value, mapItem.NpgsqlType);
            }
        }

        // This method may be used instead of NpgsqlBulkCodeBuilder
        private void PropagateDbGeneratedValues<T>(T item, MappingInfo[] infos, NpgsqlDataReader reader)
        {
            foreach (var info in infos)
            {
                info.Property.SetValue(item, ReadValue(info.Property.PropertyType, reader, info.ColumnInfo.ColumnName));
            }
        }

        public static object ReadValue(Type expectedType, NpgsqlDataReader reader, string columnName)
        {
            var value = reader[columnName];
            if (value == null)
                return value;

            var actual = value.GetType();
            if (expectedType == actual)
                return reader[columnName];
            else if (actual == typeof(DateTimeOffset) && expectedType == typeof(DateTime))
                return ((DateTimeOffset)value).DateTime;
            else
                return Convert.ChangeType(value, expectedType);
        }

        public void Insert<T>(IEnumerable<T> entities)
        {
            var conn = NpgsqlHelper.GetNpgsqlConnection(context);
            EnsureConnected(conn);
            var transaction = NpgsqlHelper.EnsureOrStartTransaction(context);

            var mapping = GetEntityInfo<T>();

            try
            {
                // 0. Prepare variables
                var dataColumns = mapping.ClientDataColumnNames.Value;
                var dbGenColumns = mapping.DbGeneratedColumnNames.Value;
                var tableName = mapping.TableNameQualified;
                var tempTableName = "_temp_" + DateTime.Now.Ticks;
                var list = entities.ToList();
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;
                if (!codeBuilder.IsInitialized)
                    codeBuilder.InitBuilder(mapping.ClientDataInfos.Value,
                        mapping.ClientDataWithKeysInfos.Value,
                        mapping.DbGeneratedInfos.Value,
                        ReadValue);

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS SELECT {dataColumns} FROM {tableName} LIMIT 0";
                context.Database.ExecuteSqlCommand(sql);

                // 2. Import into temp table
                using (var importer = conn.BeginBinaryImport($"COPY {tempTableName} ({dataColumns}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var item in list)
                    {
                        importer.StartRow();
                        //WriteRow(item, mapping.ClientDataInfos.Value, importer);
                        codeBuilder.ClientDataWriterAction(item, importer);
                    }
                }

                // 3. Insert into real table from temp one
                using (var cmd = conn.CreateCommand())
                {
                    var dbGenMappings = mapping.DbGeneratedInfos.Value;
                    cmd.CommandText = $"INSERT INTO {tableName} ({dataColumns}) SELECT {dataColumns} FROM {tempTableName} ";
                    if (dbGenMappings.Any())
                    {
                        cmd.CommandText += $"RETURNING {dbGenColumns}";
                    }
                    using (var reader = cmd.ExecuteReader())
                    {
                        // 4. Propagate computed value
                        if (dbGenMappings.Any())
                        {
                            foreach (var item in list)
                            {
                                reader.Read();
                                //PropagateDbGeneratedValues(item, dbGenMappings, reader);
                                codeBuilder.IdentityValuesWriterAction(item, reader);
                            }
                        }
                    }
                }

                // 5. Commit
                transaction?.Commit();
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
        }

        public void Update<T>(IEnumerable<T> entities)
        {
            var conn = NpgsqlHelper.GetNpgsqlConnection(context);
            EnsureConnected(conn);
            var transaction = NpgsqlHelper.EnsureOrStartTransaction(context);

            var mapping = GetEntityInfo<T>();

            try
            {
                // 0. Prepare variables
                var dataColumns = mapping.ClientDataWithKeysColumnNames.Value;
                var tableName = mapping.TableNameQualified;
                var tempTableName = "_temp_" + DateTime.Now.Ticks;
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;
                if (!codeBuilder.IsInitialized)
                    codeBuilder.InitBuilder(mapping.ClientDataInfos.Value,
                        mapping.ClientDataWithKeysInfos.Value,
                        mapping.DbGeneratedInfos.Value,
                        ReadValue);

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS SELECT {dataColumns} FROM {tableName} LIMIT 0";
                context.Database.ExecuteSqlCommand(sql);

                // 2. Import into temp table
                using (var importer = conn.BeginBinaryImport($"COPY {tempTableName} ({dataColumns}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var item in entities)
                    {
                        importer.StartRow();
                        //WriteRow(item, mapping.ClientDataWithKeysInfos.Value, importer);
                        codeBuilder.ClientDataWithKeyWriterAction(item, importer);
                    }
                }

                // 3. Insert into real table from temp one
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"UPDATE {tableName} SET {mapping.SetClause.Value} FROM {tempTableName} source WHERE {mapping.WhereClause.Value}";
                    cmd.ExecuteNonQuery();
                }

                // 5. Commit
                transaction?.Commit();
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
        }

        private List<MappingInfo> GetMappingInfo(Type type, string tableName)
        {
            var columns = GetTableSchema(tableName);
            var mappings = type.GetProperties(BindingFlags.NonPublic | BindingFlags.Public |
                    BindingFlags.GetProperty | BindingFlags.Instance)
                .Select(x => new
                {
                    PropertyInfo = x,
                    ColumnAttribute = x.GetCustomAttribute<ColumnAttribute>(),
                    DbGeneratedAttribute = x.GetCustomAttribute<DatabaseGeneratedAttribute>(),
                    KeyAttribute = x.GetCustomAttribute<KeyAttribute>(),
                    SourceAttribute = x.GetCustomAttribute<BulkMappingSourceAttribute>()
                })
                .Where(x => x.ColumnAttribute != null)
                .Select(x => new MappingInfo()
                {
                    Property = x.PropertyInfo,
                    ColumnInfo = columns.First(c => c.ColumnName == x.ColumnAttribute.Name),
                    IsDbGenerated = x.DbGeneratedAttribute != null,
                    IsKey = x.KeyAttribute != null,
                    OverrideSourceMethod = GetOverrideSouceFunc(type, x.SourceAttribute?.PropertyName)
                })
                .ToList();

            mappings.ForEach(x => x.NpgsqlType = GetNpgsqlType(x.ColumnInfo));

            return mappings;
        }

        private MethodInfo GetOverrideSouceFunc(Type t, string memberName)
        {
            if (memberName == null) return null;

            var propInfo = t.GetProperty(memberName);
            if (propInfo != null)
                return propInfo.GetGetMethod();

            return null;
        }

        private List<ColumnInfo> GetTableSchema(string tableName)
        {
            return NpgsqlHelper.GetColumnsInfo(context, tableName);
        }

        private void EnsureConnected(NpgsqlConnection conn)
        {
            if (conn.State != System.Data.ConnectionState.Open)
                conn.Open();
        }

        private EntityInfo GetEntityInfo<T>()
        {
            return Cache.GetOrAdd(typeof(T), (t) => CreateEntityInfo<T>());
        }

        private EntityInfo CreateEntityInfo<T>()
        {
            var t = typeof(T);

            var info = new EntityInfo()
            {
                TableNameQualified = GetTableNameQualified(t),
                TableName = GetTableName(t)
            };

            info.MappingInfos = new Lazy<List<MappingInfo>>(
                () => GetMappingInfo(t, info.TableName));

            info.CodeBuilder = new NpgsqlBulkCodeBuilder<T>();

            return info;
        }

        private string GetTableName(Type t)
        {
            var tableAttr = t.GetCustomAttribute<TableAttribute>();
            return tableAttr.Name;
        }

        private string GetTableNameQualified(Type t)
        {
            var tableAttr = t.GetCustomAttribute<TableAttribute>();
            return NpgsqlHelper.GetQualifiedName(tableAttr.Name, tableAttr.Schema);
        }

    }
}

