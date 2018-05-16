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
using System.Text;

namespace Npgsql.Bulk
{
    /// <summary>
    /// Uploader class itself
    /// </summary>
    public class NpgsqlBulkUploader
    {
        private static readonly ConcurrentDictionary<Type, EntityInfo> Cache = new ConcurrentDictionary<Type, EntityInfo>();
        private static readonly Dictionary<Type, object> EntityInfoLocks = new Dictionary<Type, object>();

        private readonly DbContext context;

        public NpgsqlBulkUploader(DbContext context)
        {
            this.context = context;
        }

        internal static NpgsqlDbType GetNpgsqlType(ColumnInfo info)
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
                case "citext":
                    return NpgsqlDbType.Citext;
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
                case "int2":
                case "smallint":
                    return NpgsqlDbType.Smallint;
                case "uuid":
                    return NpgsqlDbType.Uuid;
                case "timestamp":
                    return NpgsqlDbType.Timestamp;
                case "timestamptz":
                    return NpgsqlDbType.TimestampTZ;
                case "bpchar":
                    return NpgsqlDbType.Char;
                case "hstore":
                    return NpgsqlDbType.Hstore;
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
            var connOpenedHere = EnsureConnected(conn);
            var transaction = NpgsqlHelper.EnsureOrStartTransaction(context);

            var mapping = GetEntityInfo<T>();

            try
            {
                // 0. Prepare variables
                var tempTableName = "_temp_" + DateTime.Now.Ticks;
                var list = entities.ToList();
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS {mapping.SelectSourceForInsertQuery} LIMIT 0";
                //var sql = $"CREATE {tempTableName} AS {mapping.SelectSourceForInsertQuery} LIMIT 0";

                context.Database.ExecuteSqlCommand(sql);
                sql = $"ALTER TABLE {tempTableName} ADD COLUMN __index integer";
                context.Database.ExecuteSqlCommand(sql);

                // 2. Import into temp table
                using (var importer = conn.BeginBinaryImport($"COPY {tempTableName} ({mapping.CopyColumnsForInsertQueryPart}, __index) FROM STDIN (FORMAT BINARY)"))
                {
                    var index = 1;
                    foreach (var item in list)
                    {
                        importer.StartRow();
                        codeBuilder.ClientDataWriterAction(item, importer);
                        importer.Write(index, NpgsqlDbType.Integer);
                        index++;
                    }
                }

                // 3. Insert into real table from temp one
                foreach (var insertPart in mapping.InsertQueryParts)
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        var baseInsertCmd = $"INSERT INTO {insertPart.TableNameQualified} ({insertPart.TargetColumnNamesQueryPart}) SELECT {insertPart.SourceColumnNamesQueryPart} FROM {tempTableName}";
                        if (string.IsNullOrEmpty(insertPart.ReturningSetQueryPart))
                        {
                            cmd.CommandText = baseInsertCmd;
                            if (!string.IsNullOrEmpty(insertPart.Returning))
                                cmd.CommandText += $" RETURNING {insertPart.Returning}";
                        }
                        else
                        {
                            cmd.CommandText = $"WITH inserted as (\n {baseInsertCmd} RETURNING {insertPart.Returning} \n ), \n";
                            cmd.CommandText += $"source as (\n SELECT *, ROW_NUMBER() OVER (ORDER BY {insertPart.Returning}) as __index FROM inserted \n ) \n";
                            cmd.CommandText += $"UPDATE {tempTableName} SET {insertPart.ReturningSetQueryPart} FROM source WHERE {tempTableName}.__index = source.__index\n";
                            cmd.CommandText += $" RETURNING {insertPart.Returning}";
                        }

                        using (var reader = cmd.ExecuteReader())
                        {
                            // 4. Propagate computed value
                            if (!string.IsNullOrEmpty(insertPart.Returning))
                            {
                                var readAction = codeBuilder.IdentityValuesWriterActions[insertPart.TableName];
                                foreach (var item in list)
                                {
                                    reader.Read();
                                    readAction(item, reader);
                                }
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
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        public void Update<T>(IEnumerable<T> entities)
        {
            var conn = NpgsqlHelper.GetNpgsqlConnection(context);
            var connOpenedHere = EnsureConnected(conn);
            var transaction = NpgsqlHelper.EnsureOrStartTransaction(context);

            var mapping = GetEntityInfo<T>();

            try
            {
                // 0. Prepare variables
                var dataColumns = mapping.ClientDataWithKeysColumnNames;
                var tableName = mapping.TableNameQualified;
                var tempTableName = "_temp_" + DateTime.Now.Ticks;
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS {mapping.SelectSourceForUpdateQuery} LIMIT 0";
                //var sql = $"CREATE TABLE {tempTableName} AS {mapping.SelectSourceForUpdateQuery} LIMIT 0";
                context.Database.ExecuteSqlCommand(sql);

                // 2. Import into temp table
                using (var importer = conn.BeginBinaryImport($"COPY {tempTableName} ({mapping.CopyColumnsForUpdateQueryPart}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var item in entities)
                    {
                        importer.StartRow();
                        codeBuilder.ClientDataWithKeyWriterAction(item, importer);
                    }
                }

                // 3. Insert into real table from temp one
                foreach (var part in mapping.UpdateQueryParts)
                {
                    sql = $"UPDATE {part.TableNameQualified} SET {part.SetClause} FROM {tempTableName} as source WHERE {part.WhereClause}";
                    context.Database.ExecuteSqlCommand(sql);
                }

                // 5. Commit
                transaction?.Commit();
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        private List<MappingInfo> GetMappingInfo(Type type, string tableName)
        {
            var mappings = NpgsqlHelper.GetMetadata(context, type);

            mappings.ForEach(x =>
            {
                var sourceAttribute = x.Property.GetCustomAttribute<BulkMappingSourceAttribute>();
                var modifiers = x.Property.GetCustomAttributes<BulkOperationModifierAttribute>();

                x.ModifierAttributes = modifiers.ToList();
                x.OverrideSourceMethod = GetOverrideSouceFunc(type, sourceAttribute?.PropertyName);
                x.NpgsqlType = GetNpgsqlType(x.ColumnInfo);
                x.TempAliasedColumnName = $"{x.TableName}_{x.ColumnInfo.ColumnName}".ToLower();
                x.QualifiedColumnName = $"{NpgsqlHelper.GetQualifiedName(x.TableName)}.{NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName)}";
            });
            return mappings;
        }

        private MethodInfo GetOverrideSouceFunc(Type t, string memberName)
        {
            if (memberName == null) return null;

            var propInfo = t.GetProperty(memberName,
                BindingFlags.Public | BindingFlags.NonPublic |
                BindingFlags.Instance | BindingFlags.Static);
            if (propInfo != null)
            {
                if (propInfo.GetGetMethod() != null)
                {
                    return propInfo.GetMethod;
                }
                else
                {
                    return propInfo.GetMethod.CreateDelegate(
                        typeof(Func<,>).MakeGenericType(t, propInfo.PropertyType)).GetMethodInfo();
                }
            }

            return null;
        }

        private List<ColumnInfo> GetTableSchema(string tableName)
        {
            return NpgsqlHelper.GetColumnsInfo(context, tableName);
        }

        private bool EnsureConnected(NpgsqlConnection conn)
        {
            if (conn.State != System.Data.ConnectionState.Open)
            {
                conn.Open();
                return true;
            }
            return false;
        }

        private EntityInfo GetEntityInfo<T>()
        {
            var type = typeof(T);
            if (Cache.TryGetValue(type, out EntityInfo info))
            {
                return info;
            }
            else
            {
                object typeLocker;
                lock (EntityInfoLocks)
                {
                    if (!EntityInfoLocks.TryGetValue(type, out typeLocker))
                    {
                        EntityInfoLocks[type] = typeLocker = new object();
                    }
                }
                lock (typeLocker)
                {
                    info = Cache.GetOrAdd(type, (x) => CreateEntityInfo<T>());
                    EntityInfoLocks.Remove(type);
                }

                return info;
            }
        }

        private EntityInfo CreateEntityInfo<T>()
        {
            var t = typeof(T);
            var tableName = NpgsqlHelper.GetTableName(context, t);
            var mappingInfo = GetMappingInfo(t, tableName);
            var codeBuilder = new NpgsqlBulkCodeBuilder<T>();

            var info = new EntityInfo()
            {
                TableNameQualified = NpgsqlHelper.GetTableNameQualified(context, t),
                TableName = tableName,
                CodeBuilder = codeBuilder,
                MappingInfos = mappingInfo,
                TableNames = mappingInfo.Select(x => x.TableName).Distinct().ToArray(),
                ClientDataInfos = mappingInfo.Where(x => !x.IsDbGenerated).ToArray(),
                ClientDataWithKeysInfos = mappingInfo.Where(x => !x.IsDbGenerated || x.IsKey).ToArray()
            };

            var grouppedByTables = mappingInfo.GroupBy(x => x.TableName)
                .Select(x => new
                {
                    TableName = x.Key,
                    x.First().TableNameQualified,
                    KeyInfos = x.Where(y => y.IsKey).ToList(),
                    ClientDataInfos = x.Where(y => !y.IsDbGenerated).ToList(),
                    ReturningInfos = x.Where(y => y.IsDbGenerated).ToList()
                }).ToList();

            info.InsertQueryParts = grouppedByTables.Select(x =>
            {
                var others = grouppedByTables.Where(y => y.TableName != x.TableName)
                    .SelectMany(y => y.ClientDataInfos)
                    .Select(y => new
                    {
                        My = y,
                        Others = x.ReturningInfos.FirstOrDefault(ri => ri.Property.Name == y.Property.Name)
                    })
                    .Where(y => y.Others != null)
                    .ToList();

                return new InsertQueryParts()
                {
                    TableName = x.TableName,
                    TableNameQualified = x.TableNameQualified,
                    TargetColumnNamesQueryPart = string.Join(", ", x.ClientDataInfos.Select(y => NpgsqlHelper.GetQualifiedName(y.ColumnInfo.ColumnName))),
                    SourceColumnNamesQueryPart = string.Join(", ", x.ClientDataInfos.Select(y => y.TempAliasedColumnName)),
                    Returning = string.Join(", ", x.ReturningInfos.Select(y => y.ColumnInfo.ColumnName)),
                    ReturningSetQueryPart = string.Join(", ", others.Select(y => $"{y.My.TempAliasedColumnName} = source.{y.Others.ColumnInfo.ColumnName}"))
                };
            }).ToList();

            info.SelectSourceForInsertQuery = "SELECT " +
                string.Join(", ", info.ClientDataInfos
                    .Select(x => $"{x.QualifiedColumnName} AS {x.TempAliasedColumnName}")) +
                " FROM " + string.Join(", ", grouppedByTables.Select(x => x.TableNameQualified));
            info.CopyColumnsForInsertQueryPart = string.Join(", ", info.ClientDataInfos
                .Select(x => x.TempAliasedColumnName));

            info.UpdateQueryParts = grouppedByTables.Select(x =>
            {
                var updateableInfos = x.ClientDataInfos;
                updateableInfos = updateableInfos.Where(
                    y => y.ModifierAttributes == null ||
                        y.ModifierAttributes.All(
                            m => m.Modification != BulkOperationModification.IgnoreForUpdate)
                    ).ToList();

                return new UpdateQueryParts()
                {
                    TableName = x.TableName,
                    TableNameQualified = x.TableNameQualified,
                    SetClause = string.Join(", ", updateableInfos.Select(y =>
                    {
                        var colName = NpgsqlHelper.GetQualifiedName(y.ColumnInfo.ColumnName);
                        return $"{colName} = source.{y.TempAliasedColumnName}";
                    })),
                    WhereClause = string.Join(" and ", x.KeyInfos.Select(y =>
                    {
                        var colName = NpgsqlHelper.GetQualifiedName(y.ColumnInfo.ColumnName);
                        return $"{colName} = source.{y.TempAliasedColumnName}";
                    }))
                };
            }).ToList();

            info.SelectSourceForUpdateQuery = "SELECT " +
                string.Join(", ", info.ClientDataWithKeysInfos
                    .Select(x => $"{x.QualifiedColumnName} AS {x.TempAliasedColumnName}")) +
                " FROM " + string.Join(", ", grouppedByTables.Select(x => x.TableNameQualified));
            info.CopyColumnsForUpdateQueryPart = string.Join(", ", info.ClientDataWithKeysInfos
                .Select(x => x.TempAliasedColumnName));

            info.ClientDataColumnNames = string.Join(", ",
                info.ClientDataInfos.Select(x => NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName)));

            info.KeyInfos = info.MappingInfos.Where(x => x.IsKey).ToArray();

            info.KeyColumnNames = info.KeyInfos.Select(x => x.ColumnInfo.ColumnName).ToArray();

            info.ClientDataWithKeysColumnNames = string.Join(", ",
                info.ClientDataWithKeysInfos.Select(x => NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName)));

            info.DbGeneratedInfos = info.MappingInfos.Where(x => x.IsDbGenerated).ToArray();

            info.DbGeneratedColumnNames = string.Join(", ",
                info.DbGeneratedInfos.Select(x => NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName)));

            codeBuilder.InitBuilder(info.ClientDataInfos,
                info.ClientDataWithKeysInfos,
                info.DbGeneratedInfos,
                ReadValue);

            return info;
        }
    }
}

