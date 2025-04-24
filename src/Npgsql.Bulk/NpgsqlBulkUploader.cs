﻿using NpgsqlTypes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
#if EFCore
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
#else
using System.Data.Entity;
#endif
using System.Linq;
using System.Reflection;
using Npgsql.Bulk.Model;
using System.Data;
using System.Threading.Tasks;
using System.Reflection.Emit;
using System.Threading;
using System.Linq.Expressions;
#if NET8_0
using Npgsql.Internal;
#endif
using Npgsql.TypeMapping;

namespace Npgsql.Bulk
{
    /// <summary>
    /// Uploader class itself
    /// </summary>
    public class NpgsqlBulkUploader
    {
        private static readonly ConcurrentDictionary<string, EntityInfo> Cache = new ConcurrentDictionary<string, EntityInfo>();
        private static readonly Dictionary<string, object> EntityInfoLocks = new Dictionary<string, object>();

        private readonly DbContext context;
        private readonly bool disableEntitiesTracking;
        private static string uniqueTablePrefix = Guid.NewGuid().ToString().Replace("-", "_");
        private static int tablesCounter = 0;
        private static readonly ConcurrentDictionary<string, object> partialCodeBuilders = new ConcurrentDictionary<string, object>();

        private static MethodInfo CompleteMethodInfo = typeof(NpgsqlBinaryImporter).GetMethod("Complete");

        internal static IRelationalHelper RelationalHelper = new RelationalHelper();

        /// <summary>
        /// When transaction needs to be started internally then this IsolationLevel will be used
        /// </summary>
        public IsolationLevel DefaultIsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

        /// <summary>
        /// Means what level of table lock needs to be accured when doing Update
        /// </summary>
        public TableLockLevel LockLevelOnUpdate { get; set; } = TableLockLevel.ShareRowExclusive;

        /// <summary>
        /// Allows to override command execution timeout
        /// </summary>
        public int? CommandTimeout { get; set; }

        public NpgsqlBulkUploader(DbContext context, bool disableEntitiesTracking) : this(context)
        {
            this.disableEntitiesTracking = disableEntitiesTracking;
        }

        public NpgsqlBulkUploader(DbContext context)
        {
            this.context = context;
        }

#if EFCore
        internal NpgsqlDbType GetNpgsqlType(ColumnInfo info)
#else
        internal static NpgsqlDbType GetNpgsqlType(ColumnInfo info)
#endif
        {
            switch (info.ColumnType)
            {
                case "interval":
                    return NpgsqlDbType.Interval;
                case "integer":
                case "int":
                case "int4":
                    return NpgsqlDbType.Integer;
                case "bool":
                case "boolean":
                    return NpgsqlDbType.Boolean;
                case "box":
                    return NpgsqlDbType.Box;
                case "circle":
                    return NpgsqlDbType.Circle;
                case "line":
                    return NpgsqlDbType.Line;
                case "lseg":
                    return NpgsqlDbType.LSeg;
                case "money":
                    return NpgsqlDbType.Money;
                case "path":
                    return NpgsqlDbType.Path;
                case "point":
                    return NpgsqlDbType.Point;
                case "polygon":
                    return NpgsqlDbType.Polygon;
                case "inet":
                    return NpgsqlDbType.Inet;
                case "bit":
                    return NpgsqlDbType.Bit;
                case "varchar":
                    return NpgsqlDbType.Varchar;
                case "char":
                    return NpgsqlDbType.Char;
                case "real":
                case "float4":
                    return NpgsqlDbType.Real;
                case "float8":
                case "double":
                case "double precision":
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
                    return NpgsqlDbType.TimeTz;
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
                    return NpgsqlDbType.TimestampTz;
                case "bpchar":
                    return NpgsqlDbType.Char;
                case "hstore":
                    return NpgsqlDbType.Hstore;
                case "json":
                    return NpgsqlDbType.Json;
                case "jsonb":
                    return NpgsqlDbType.Jsonb;
                case "_text":
                    return NpgsqlDbType.Array;
                case "bytea":
                    return NpgsqlDbType.Bytea;
                case "tsrange":
                case "int4range":
                case "int8range":
                case "numrange":
                case "tstzrange":
                case "daterange":
                    return NpgsqlDbType.Range;
                default:

                    if (Enum.TryParse<NpgsqlDbType>(info.ColumnType, true, out var npgsqlDbType))
                        return npgsqlDbType;

                    if (string.Equals(info?.ColumnTypeExtra, "array", StringComparison.OrdinalIgnoreCase) || info.ColumnType.StartsWith("_"))
                        return NpgsqlDbType.Array;

#if DotNet6
                    // Allow postgres enum types to be mapped to CLR enums
                    var mapper = RelationalHelper.GetNpgsqlConnection(context).TypeMapper;
                    var handlers = (ConcurrentDictionary<string, Internal.TypeHandling.NpgsqlTypeHandler>)
                        mapper.GetType()
                        .GetField("_handlersByDataTypeName", BindingFlags.Instance | BindingFlags.NonPublic)
                        .GetValue(mapper);

                    if (handlers.TryGetValue(info.ColumnType, out var _))
                    {
                        return NpgsqlDbType.Unknown;
                    }
#elif DotNet7
                    // Allow postgres enum types to be mapped to CLR enums
                    var mapper = NpgsqlConnection.GlobalTypeMapper;
                    var userTypeMappings = (ConcurrentDictionary<string, Internal.TypeMapping.IUserTypeMapping>)
                        mapper.GetType().GetProperty("UserTypeMappings").GetValue(mapper);
                    if (userTypeMappings.ContainsKey(info.ColumnType))
                    {
                        return NpgsqlDbType.Unknown;
                    }
#elif NET8_0
                    // Allow postgres enum types to be mapped to CLR enums
                    var mapper = NpgsqlConnection.GlobalTypeMapper;
                    var userTypeMapper = mapper.GetType()
                        .GetField("_userTypeMapper", BindingFlags.Instance | BindingFlags.NonPublic)
                        .GetValue(mapper);
                    var userTypeMappings = (IList<UserTypeMapping>)userTypeMapper.GetType().GetProperty("Items").GetValue(userTypeMapper);

                    if (userTypeMappings != null && userTypeMappings.Any(x => x.PgTypeName == info.ColumnType))
                    {
                        return NpgsqlDbType.Unknown;
                    }
#elif NET9_0
                    // Allow postgres enum types to be mapped to CLR enums
                    var mapper = NpgsqlConnection.GlobalTypeMapper;
                    var userTypeMapper = mapper.GetType()
                        .GetField("_userTypeMapper", BindingFlags.Instance | BindingFlags.NonPublic)
                        .GetValue(mapper);
                    var userTypeMappings = (IList<UserTypeMapping>)userTypeMapper.GetType().GetProperty("Items").GetValue(userTypeMapper);

                    if (userTypeMappings != null && userTypeMappings.Any(x => x.PgTypeName == info.ColumnType))
                    {
                        return NpgsqlDbType.Unknown;
                    }
#elif EFCore
                    // Allow postgres enum types to be mapped to CLR enums
                    var clrType = RelationalHelper.GetNpgsqlConnection(context).TypeMapper.Mappings
                        .FirstOrDefault(mapping => mapping.PgTypeName == info.ColumnType)?
                        .ClrTypes
                        .FirstOrDefault();
                    if (clrType != null && clrType.IsEnum)
                    {
                        // NpgsqlDbType.Unknown is a dummy value.  The actual postgres type
                        // will be determined later by Npgsql.
                        return NpgsqlDbType.Unknown;
                    }
#endif

                    throw new NotImplementedException($"Column type '{info.ColumnType}' is not supported");
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
            if (value == DBNull.Value)
                return null;
            else if (expectedType == actual)
                return reader[columnName];
            else if (actual == typeof(DateTimeOffset) && expectedType == typeof(DateTime))
                return ((DateTimeOffset)value).DateTime;
            else if (expectedType.IsEnum && value is int)
                return Enum.ToObject(expectedType, (int)value);
            else
            {
                var nullableSubtype = Nullable.GetUnderlyingType(expectedType);
                return Convert.ChangeType(value, nullableSubtype ?? expectedType);
            }
        }

        public void Insert<T>(IEnumerable<T> entities)
        {
            Insert<T>(entities, null);
        }

#if EFCore
        private (T[] attached, T[] modified) SetAutoGeneratedFields<T>(
            IEnumerable<T> entities,
            EntityInfo infos,
            NpgsqlBulkCodeBuilder<T> codeBuilders,
            EntityState state)
        {
            if (infos.PropertyNameToGenerators == null ||
                infos.PropertyNameToGenerators.Count == 0 ||
                disableEntitiesTracking)
                return (Array.Empty<T>(), Array.Empty<T>());

            var sm = ((IDbContextDependencies)context).StateManager;
            var attached = new List<T>();
            var modified = new List<T>();

            foreach (var item in entities)
            {
                var entry = sm.TryGetEntry(item);
                if (entry == null)
                {
                    entry = sm.GetOrCreateEntry(item);
                    entry.SetEntityState(EntityState.Added);
                    attached.Add(item);

                    //codeBuilders.AutoGenerateValues(
                    //    item, new Microsoft.EntityFrameworkCore.ChangeTracking.EntityEntry(entry), infos.PropertyNameToGenerators);
                }
                else
                {
                    modified.Add(item);
                }
            }

            return (attached.ToArray(), modified.ToArray());
        }

        private void DetachEntries<T>(T[] attached, T[] modified)
        {
            if (!attached.Any())
                return;

            var sm = ((IDbContextDependencies)context).StateManager;

            foreach (var item in attached)
            {
                var entry = sm.TryGetEntry(item);
                entry.SetEntityState(EntityState.Detached);
            }

            foreach (var item in modified)
            {
                var entry = sm.TryGetEntry(item);
                entry.AcceptChanges();
            }
        }
#endif

        public void Insert<T>(IEnumerable<T> entities, InsertConflictAction onConflict)
        {
            var conn = RelationalHelper.GetNpgsqlConnection(context);
            var connOpenedHere = EnsureConnected(conn);
            var transaction = RelationalHelper.EnsureOrStartTransaction(context, DefaultIsolationLevel);
            var mapping = GetEntityInfo<T>();

            // var ignoreDuplicatesStatement = onConflict?.GetSql(mapping);


            try
            {
                // 0. Prepare variables
                var tempTableName = GetUniqueName("_temp_");
                var list = entities.ToList();
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS {mapping.SelectSourceForInsertQuery} LIMIT 0";
                //var sql = $"CREATE {tempTableName} AS {mapping.SelectSourceForInsertQuery} LIMIT 0";

                ExecuteNonQuery(conn, sql);
                sql = $"ALTER TABLE {tempTableName} ADD COLUMN __index integer";
                ExecuteNonQuery(conn, sql);

#if EFCore
                var (attached, modified) = SetAutoGeneratedFields(list, mapping, codeBuilder, EntityState.Added);
#endif

                // 
                if (mapping.MaxIsOptionalFlag == 0)
                {
                    WriteInsertPortion(list, mapping, conn, tempTableName, codeBuilder);

                    InsertPortion<T>(list, mapping.InsertQueryParts[0], conn, codeBuilder,
                        tempTableName, mapping, onConflict);
                }
                else
                {
                    var classified = list.ToLookup(x => codeBuilder.ClassifyOptionals(x));

                    foreach (var bucket in classified)
                    {
                        ExecuteNonQuery(conn, $"TRUNCATE TABLE " + tempTableName);

                        WriteInsertPortion(bucket, mapping, conn, tempTableName, codeBuilder);

                        InsertPortion<T>(
                            bucket,
                            mapping.InsertQueryParts.GetOrAdd(bucket.Key, (key) => GetInsertQueryParts(mapping.MappingInfos, key)),
                            conn,
                            codeBuilder,
                            tempTableName,
                            mapping,
                            onConflict);
                    }
                }

                // 5. Commit
                transaction?.Commit();

#if EFCore
                DetachEntries(attached, modified);
#endif
            }
            catch
            {
                try
                {
                    transaction?.Rollback();
                }
                catch { }

                throw;
            }
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        private void WriteInsertPortion<T>(
            IEnumerable<T> list,
            EntityInfo mapping,
            NpgsqlConnection conn,
            string tempTableName,
            NpgsqlBulkCodeBuilder<T> codeBuilder)
        {
            // 2. Import into temp table
            using (var importer = conn.BeginBinaryImport($"COPY {tempTableName} ({mapping.CopyColumnsForInsertQueryPart}, __index) FROM STDIN (FORMAT BINARY)"))
            {
                var opContext = new OperationContext(context, false);

                var index = 1;
                foreach (var item in list)
                {
                    importer.StartRow();
                    codeBuilder.WriterForInsertAction(item, importer, opContext);
                    importer.Write(index, NpgsqlDbType.Integer);
                    index++;
                }

                // Temp solution!!!
                //importer.Complete();
                CompleteMethodInfo.Invoke(importer, null);
            }
        }

        private void InsertPortion<T>(
            IEnumerable<T> list,
            List<InsertQueryParts> insertParts,
            NpgsqlConnection conn,
            NpgsqlBulkCodeBuilder<T> codeBuilder,
            string tempTableName,
            EntityInfo entityInfo,
            InsertConflictAction onConflict)
        {
            // 3. Insert into real table from temp one
            foreach (var insertPart in insertParts)
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;

                    var ignoreDuplicatesStatement = onConflict?.GetSql(entityInfo, insertPart.TableNameQualified);

                    var baseInsertCmd = $"INSERT INTO {insertPart.TableNameQualified} ({insertPart.TargetColumnNamesQueryPart}) " +
                        $"SELECT {insertPart.SourceColumnNamesQueryPart} FROM {tempTableName} ORDER BY __index {ignoreDuplicatesStatement}";

                    if (string.IsNullOrEmpty(insertPart.ReturningSetQueryPart))
                    {
                        cmd.CommandText = baseInsertCmd;

                        if (!string.IsNullOrEmpty(insertPart.Returning))
                        {
                            cmd.CommandText = $"WITH inserted as (\n {baseInsertCmd} RETURNING {insertPart.Returning} \n ), \n";
                            cmd.CommandText += $"source as (\n SELECT *, ROW_NUMBER() OVER () as __index FROM inserted \n ) \n";
                            cmd.CommandText += $"SELECT * FROM source ORDER BY __index";
                        }
                    }
                    else
                    {
                        cmd.CommandText = $"WITH inserted as (\n {baseInsertCmd} RETURNING {insertPart.Returning} \n ), \n";
                        cmd.CommandText += $"source as (\n SELECT *, ROW_NUMBER() OVER () as __index FROM inserted \n ) \n";
                        cmd.CommandText += $"UPDATE {tempTableName} SET {insertPart.ReturningSetQueryPart} FROM source WHERE {tempTableName}.__index = source.__index\n";
                        cmd.CommandText += $"RETURNING {insertPart.Returning}";
                    }

                    using (var reader = cmd.ExecuteReader(CommandBehavior.Default))
                    {
                        // 4. Propagate computed value
                        if (!string.IsNullOrEmpty(insertPart.Returning))
                        {
                            if (onConflict == null)
                            {
                                var readAction = codeBuilder.InsertIdentityValuesWriterActions[insertPart.TableName];
                                foreach (var item in list)
                                {
                                    reader.Read();
                                    readAction(item, reader);
                                }
                            }
                            else
                            {
                                while (reader.Read())
                                {
                                    // do nothing, for now...
                                }
                            }
                        }
                    }
                }
            }
        }

        private async Task InsertPortionAsync<T>(
            IEnumerable<T> list,
            List<InsertQueryParts> insertParts,
            NpgsqlConnection conn,
            NpgsqlBulkCodeBuilder<T> codeBuilder,
            string tempTableName,
            EntityInfo entityInfo,
            InsertConflictAction onConflict)
        {
            // 3. Insert into real table from temp one
            foreach (var insertPart in insertParts)
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;

                    var ignoreDuplicatesStatement = onConflict?.GetSql(entityInfo, insertPart.TableNameQualified);

                    var baseInsertCmd = $"INSERT INTO {insertPart.TableNameQualified} ({insertPart.TargetColumnNamesQueryPart}) " +
                        $"SELECT {insertPart.SourceColumnNamesQueryPart} FROM {tempTableName} ORDER BY __index {ignoreDuplicatesStatement}";

                    if (string.IsNullOrEmpty(insertPart.ReturningSetQueryPart))
                    {
                        cmd.CommandText = baseInsertCmd;

                        if (!string.IsNullOrEmpty(insertPart.Returning))
                        {
                            cmd.CommandText = $"WITH inserted as (\n {baseInsertCmd} RETURNING {insertPart.Returning} \n ), \n";
                            cmd.CommandText += $"source as (\n SELECT *, ROW_NUMBER() OVER (ORDER BY {insertPart.Returning}) as __index FROM inserted \n ) \n";
                            cmd.CommandText += $"SELECT * FROM source ORDER BY __index";
                        }
                    }
                    else
                    {
                        cmd.CommandText = $"WITH inserted as (\n {baseInsertCmd} RETURNING {insertPart.Returning} \n ), \n";
                        cmd.CommandText += $"source as (\n SELECT *, ROW_NUMBER() OVER (ORDER BY {insertPart.Returning}) as __index FROM inserted \n ) \n";
                        cmd.CommandText += $"UPDATE {tempTableName} SET {insertPart.ReturningSetQueryPart} FROM source WHERE {tempTableName}.__index = source.__index\n";
                        cmd.CommandText += $"RETURNING {insertPart.Returning}";
                    }

                    using (var reader = (NpgsqlDataReader)(await cmd.ExecuteReaderAsync()))
                    {
                        // 4. Propagate computed value
                        if (!string.IsNullOrEmpty(insertPart.Returning))
                        {
                            if (onConflict == null)
                            {
                                var readAction = codeBuilder.InsertIdentityValuesWriterActions[insertPart.TableName];
                                foreach (var item in list)
                                {
                                    await reader.ReadAsync();
                                    readAction(item, reader);
                                }
                            }
                            else
                            {
                                while (await reader.ReadAsync())
                                {
                                    // do nothing, for now...
                                }
                            }
                        }
                    }
                }
            }
        }

        private static string LockLevelToString(TableLockLevel lockMode)
        {
            switch (lockMode)
            {
                case TableLockLevel.ShareRowExclusive:
                    return "SHARE ROW EXCLUSIVE";
                case TableLockLevel.Exclusive:
                    return "EXCLUSIVE";
                case TableLockLevel.AccessExclusive:
                    return "ACCESS EXCLUSIVE";
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// Bulk update entities
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        public void Update<T>(IEnumerable<T> entities)
        {
            UpdateInternal<T>(entities, GetEntityInfo<T>());
        }

        /// <summary>
        /// Bulk update entities of specified properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="propertiesToUpdate"></param>
        public void Update<T>(IEnumerable<T> entities,
            params Expression<Func<T, Object>>[] propertiesToUpdate)
        {
            var mapping = CreateEntityInfoForPartialUpdate<T>(
                propertiesToUpdate.Select(x => InsertConflictAction.UnwrapProperty<T>(x)).ToArray());
            UpdateInternal<T>(entities, mapping);
        }

        /// <summary>
        /// Bulk update entities of specified properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="propertiesToUpdate"></param>
        public void Update<T>(IEnumerable<T> entities, IEnumerable<PropertyInfo> propertiesToUpdate)
        {
            var mapping = CreateEntityInfoForPartialUpdate<T>(propertiesToUpdate);
            UpdateInternal<T>(entities, mapping);
        }

        private void UpdateInternal<T>(IEnumerable<T> entities, EntityInfo mapping)
        {
            var conn = RelationalHelper.GetNpgsqlConnection(context);
            var connOpenedHere = EnsureConnected(conn);
            var transaction = RelationalHelper.EnsureOrStartTransaction(context, DefaultIsolationLevel);

            try
            {
                // 0. Prepare variables
                var tableName = mapping.TableNameQualified;
                var tempTableName = GetUniqueName("_temp_");
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;
                var opContext = new OperationContext(context, false);

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS {mapping.SelectSourceForUpdateQuery} LIMIT 0";

                ExecuteNonQuery(conn, sql);

                // 2. Import into temp table
                using (var importer = conn.BeginBinaryImport($"COPY {tempTableName} ({mapping.CopyColumnsForUpdateQueryPart}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var item in entities)
                    {
                        importer.StartRow();
                        codeBuilder.WriterForUpdateAction(item, importer, opContext);
                    }

                    // Temp solution!!!
                    //importer.Complete();
                    CompleteMethodInfo.Invoke(importer, null);
                }

                // 3. Insert into real table from temp one
                foreach (var part in mapping.UpdateQueryParts)
                {
                    // 3.a Needs to accure lock
                    if (LockLevelOnUpdate != TableLockLevel.NoLock)
                    {
                        sql = $"LOCK TABLE {part.TableNameQualified} IN {LockLevelToString(LockLevelOnUpdate)} MODE;";
                        ExecuteNonQuery(conn, sql);
                    }
                    sql = $"UPDATE {part.TableNameQualified} SET {part.SetClause} FROM {tempTableName} as source WHERE {part.WhereClause}";

                    ExecuteNonQuery(conn, sql);
                }

                // 5. Commit
                transaction?.Commit();
            }
            catch
            {
                try
                {
                    transaction?.Rollback();
                }
                catch { }

                throw;
            }
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        /// <summary>
        /// Simplified version of Insert which works better for huge sets (not calling ToList internally).
        /// Note: it imports directly to target table, doesn't use RETURING, doesn't support inheritance
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="onConflict"></param>
        public void Import<T>(IEnumerable<T> entities)
        {
            var mapping = GetEntityInfo<T>();
            if (mapping.InsertQueryParts[0].Count > 1)
                throw new NotSupportedException($"Import doesn't support entities with inheritance for now");

            var conn = RelationalHelper.GetNpgsqlConnection(context);
            var connOpenedHere = EnsureConnected(conn);
            var transaction = RelationalHelper.EnsureOrStartTransaction(context, DefaultIsolationLevel);

            try
            {
                // Prepare variables
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;
                var opContext = new OperationContext(context, true);

                // Import
                using (var importer = conn.BeginBinaryImport($"COPY {mapping.TableNameQualified} ({mapping.InsertQueryParts[0][0].TargetColumnNamesQueryPart}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var item in entities)
                    {
                        importer.StartRow();
                        codeBuilder.WriterForInsertAction(item, importer, opContext);
                    }

                    // Temp solution!!!
                    //importer.Complete();
                    CompleteMethodInfo.Invoke(importer, null);
                }

                // Commit
                transaction?.Commit();
            }
            catch
            {
                try
                {
                    transaction?.Rollback();
                }
                catch { }

                throw;
            }
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        #region Async versions

        public Task InsertAsync<T>(IEnumerable<T> entities)
        {
            return InsertAsync<T>(entities, null);
        }

        public async Task InsertAsync<T>(IEnumerable<T> entities, InsertConflictAction onConflict)
        {
            var conn = RelationalHelper.GetNpgsqlConnection(context);
            var connOpenedHere = await EnsureConnectedAsync(conn);
            var transaction = RelationalHelper.EnsureOrStartTransaction(context, DefaultIsolationLevel);
            var mapping = GetEntityInfo<T>();

            try
            {
                // 0. Prepare variables
                var tempTableName = GetUniqueName("_temp_");
                var list = entities.ToList();
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS {mapping.SelectSourceForInsertQuery} LIMIT 0";
                //var sql = $"CREATE {tempTableName} AS {mapping.SelectSourceForInsertQuery} LIMIT 0";

                await ExecuteNonQueryAsync(conn, sql);
                sql = $"ALTER TABLE {tempTableName} ADD COLUMN __index integer";
                await ExecuteNonQueryAsync(conn, sql);

#if EFCore
                var (attached, modified) = SetAutoGeneratedFields(list, mapping, codeBuilder, EntityState.Added);
#endif

                if (mapping.MaxIsOptionalFlag == 0)
                {
                    WriteInsertPortion(list, mapping, conn, tempTableName, codeBuilder);

                    await InsertPortionAsync<T>(list, mapping.InsertQueryParts[0], conn, codeBuilder,
                        tempTableName, mapping, onConflict);
                }
                else
                {
                    var classified = list.ToLookup(x => codeBuilder.ClassifyOptionals(x));

                    foreach (var bucket in classified)
                    {
                        await ExecuteNonQueryAsync(conn, $"TRUNCATE TABLE " + tempTableName);

                        WriteInsertPortion(bucket, mapping, conn, tempTableName, codeBuilder);

                        await InsertPortionAsync<T>(
                            bucket,
                            mapping.InsertQueryParts.GetOrAdd(bucket.Key, (key) => GetInsertQueryParts(mapping.MappingInfos, key)),
                            conn,
                            codeBuilder,
                            tempTableName,
                            mapping,
                            onConflict);
                    }
                }

                // 5. Commit
                transaction?.Commit();

#if EFCore
                DetachEntries(attached, modified);
#endif
            }
            catch
            {
                try
                {
                    transaction?.Rollback();
                }
                catch { }

                throw;
            }
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        public Task UpdateAsync<T>(IEnumerable<T> entities)
        {
            return UpdateAsyncInternal<T>(entities, GetEntityInfo<T>());
        }

        /// <summary>
        /// Bulk update entities of specified properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="propertiesToUpdate"></param>
        public Task UpdateAsync<T>(IEnumerable<T> entities,
            params Expression<Func<T, Object>>[] propertiesToUpdate)
        {
            var mapping = CreateEntityInfoForPartialUpdate<T>(
                propertiesToUpdate.Select(x => InsertConflictAction.UnwrapProperty<T>(x)).ToArray());
            return UpdateAsyncInternal<T>(entities, mapping);
        }

        /// <summary>
        /// Bulk update entities of specified properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="propertiesToUpdate"></param>
        public Task UpdateAsync<T>(IEnumerable<T> entities, IEnumerable<PropertyInfo> propertiesToUpdate)
        {
            var mapping = CreateEntityInfoForPartialUpdate<T>(propertiesToUpdate);
            return UpdateAsyncInternal<T>(entities, mapping);
        }

        private async Task UpdateAsyncInternal<T>(IEnumerable<T> entities, EntityInfo mapping)
        {
            var conn = RelationalHelper.GetNpgsqlConnection(context);
            var connOpenedHere = await EnsureConnectedAsync(conn);
            var transaction = RelationalHelper.EnsureOrStartTransaction(context, DefaultIsolationLevel);

            try
            {
                // 0. Prepare variables
                var tableName = mapping.TableNameQualified;
                var tempTableName = GetUniqueName("_temp_");
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;
                var opContext = new OperationContext(context, false);

                // 1. Create temp table 
                var sql = $"CREATE TEMP TABLE {tempTableName} ON COMMIT DROP AS {mapping.SelectSourceForUpdateQuery} LIMIT 0";
                //var sql = $"CREATE TABLE {tempTableName} AS {mapping.SelectSourceForUpdateQuery} LIMIT 0";

                await ExecuteNonQueryAsync(conn, sql);

                // 2. Import into temp table
                using (var importer = conn.BeginBinaryImport($"COPY {tempTableName} ({mapping.CopyColumnsForUpdateQueryPart}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var item in entities)
                    {
                        importer.StartRow();
                        codeBuilder.WriterForUpdateAction(item, importer, opContext);
                    }

                    // Temp solution!!!
                    //importer.Complete();
                    CompleteMethodInfo.Invoke(importer, null);
                }

                // 3. Insert into real table from temp one
                foreach (var part in mapping.UpdateQueryParts)
                {
                    // 3.a Needs to accure lock
                    if (LockLevelOnUpdate != TableLockLevel.NoLock)
                    {
                        sql = $"LOCK TABLE {part.TableNameQualified} IN {LockLevelToString(LockLevelOnUpdate)} MODE;";
                        await ExecuteNonQueryAsync(conn, sql);
                    }

                    sql = $"UPDATE {part.TableNameQualified} SET {part.SetClause} FROM {tempTableName} as source WHERE {part.WhereClause}";
                    await ExecuteNonQueryAsync(conn, sql);
                }

                // 5. Commit
                transaction?.Commit();
            }
            catch
            {
                try
                {
                    transaction?.Rollback();
                }
                catch { }

                throw;
            }
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        /// <summary>
        /// Simplified version of Insert which works better for huge sets (not calling ToList internally).
        /// Note: it imports directly to target table, doesn't use RETURING, doesn't support inheritance
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="onConflict"></param>
        public async Task ImportAsync<T>(IEnumerable<T> entities)
        {
            var mapping = GetEntityInfo<T>();
            if (mapping.InsertQueryParts[0].Count > 1)
                throw new NotSupportedException($"Import doesn't support entities with inheritance for now");

            var conn = RelationalHelper.GetNpgsqlConnection(context);
            var connOpenedHere = await EnsureConnectedAsync(conn);
            var transaction = RelationalHelper.EnsureOrStartTransaction(context, DefaultIsolationLevel);

            try
            {
                // Prepare variables
                var codeBuilder = (NpgsqlBulkCodeBuilder<T>)mapping.CodeBuilder;
                var opContext = new OperationContext(context, true);

                // Import
                using (var importer = conn.BeginBinaryImport($"COPY {mapping.TableNameQualified} ({mapping.InsertQueryParts[0][0].TargetColumnNamesQueryPart}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var item in entities)
                    {
                        importer.StartRow();
                        codeBuilder.WriterForInsertAction(item, importer, opContext);
                    }

                    // Temp solution!!!
                    //importer.Complete();
                    CompleteMethodInfo.Invoke(importer, null);
                }

                // Commit
                transaction?.Commit();
            }
            catch
            {
                try
                {
                    transaction?.Rollback();
                }
                catch { }

                throw;
            }
            finally
            {
                if (connOpenedHere)
                    conn.Close();
            }
        }

        #endregion

        private void ExecuteNonQuery(Npgsql.NpgsqlConnection connection, string command)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;

                cmd.CommandText = command;
                cmd.ExecuteNonQuery();
            }
        }

        private async Task ExecuteNonQueryAsync(Npgsql.NpgsqlConnection connection, string command)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;

                cmd.CommandText = command;
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private List<MappingInfo> GetMappingInfo(Type type, string tableName)
        {
            var mappings = NpgsqlHelper.GetMetadata(context, type);

            mappings.ForEach(x =>
            {
                var sourceAttribute = x.Property?.GetCustomAttribute<BulkMappingSourceAttribute>();
                var modifiers = x.Property?.GetCustomAttributes<BulkOperationModifierAttribute>();

                x.ModifierAttributes = modifiers?.ToList() ?? new List<BulkOperationModifierAttribute>();
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
            return RelationalHelper.GetColumnsInfo(context, tableName);
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

        private async ValueTask<bool> EnsureConnectedAsync(NpgsqlConnection conn)
        {
            if (conn.State != System.Data.ConnectionState.Open)
            {
                await conn.OpenAsync();
                return true;
            }
            return false;
        }

        private EntityInfo GetEntityInfo<T>()
        {
            var schema = NpgsqlHelper.GetTableSchema(context, typeof(T));

            var key = $"{schema}-{context.GetType().FullName}-{typeof(T).FullName}";
            if (Cache.TryGetValue(key, out EntityInfo info))
            {
                return info;
            }
            else
            {
                object typeLocker;
                lock (EntityInfoLocks)
                {
                    if (!EntityInfoLocks.TryGetValue(key, out typeLocker))
                    {
                        EntityInfoLocks[key] = typeLocker = new object();
                    }
                }
                lock (typeLocker)
                {
                    info = Cache.GetOrAdd(key, (x) => CreateEntityInfo<T>());
                    EntityInfoLocks.Remove(key);
                }

                return info;
            }
        }

        internal EntityInfo CreateEntityInfo<T>()
        {
            var t = typeof(T);
            var tableName = NpgsqlHelper.GetTableName(context, t);
            var mappingInfo = GetMappingInfo(t, tableName);
            var tableNameQualified = NpgsqlHelper.GetTableNameQualified(context, t);

            return CreateEntityInfo<T>(tableName, tableNameQualified, mappingInfo);
        }

        private EntityInfo CreateEntityInfoForPartialUpdate<T>(IEnumerable<PropertyInfo> props)
        {
            var t = typeof(T);
            var tableName = NpgsqlHelper.GetTableName(context, t);
            var mappingInfo = GetMappingInfo(t, tableName);
            var tableNameQualified = NpgsqlHelper.GetTableNameQualified(context, t);

            // filter by props
            var keyProps = mappingInfo.Where(x => x.IsKey).Select(x => x.Property);
            var propToInfo = mappingInfo.Where(x => x.Property != null).ToDictionary(x => x.Property.Name);
            mappingInfo = props.Union(keyProps).Select(x => propToInfo[x.Name]).ToList();

            // key for partial coe builder
            var propNames = typeof(T).FullName + "_" +
                string.Join("_", props.Select(x => $"{x.DeclaringType.Name}_{x.Name}"));
            NpgsqlBulkCodeBuilder<T> codeBuilder;
            if (partialCodeBuilders.TryGetValue(propNames, out object cb))
            {
                codeBuilder = (NpgsqlBulkCodeBuilder<T>)cb;
                return CreateEntityInfo<T>(tableName, tableNameQualified, mappingInfo, codeBuilder);
            }
            else
            {
                // Make sure primary code builder exists
                GetEntityInfo<T>();

                codeBuilder = new NpgsqlBulkCodeBuilder<T>();
                var info = CreateEntityInfo<T>(tableName, tableNameQualified, mappingInfo, codeBuilder);
                codeBuilder.InitBuilder(info, true, ReadValue);
                partialCodeBuilders.TryAdd(propNames, codeBuilder);

                return info;
            }
        }

        private List<InsertQueryParts> GetInsertQueryParts(
            List<MappingInfo> mappingInfo,
            long optionalFlags)
        {
            var grouppedByTables = mappingInfo
                .GroupBy(x => x.TableName)
                .Select(x => new
                {
                    TableName = x.Key,
                    x.First().TableNameQualified,
                    KeyInfos = x.Where(y => y.IsKey).ToList(),
                    ClientDataInfos = x.Where(y => y.DoInsert &&
                        (y.IsSpecifiedFlag == 0 || (y.IsSpecifiedFlag & optionalFlags) > 0)).ToList(),
                    ReturningInfos = x.Where(y => y.ReadBack).ToList()
                })
                .ToList();

            return grouppedByTables.Select(x =>
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
                    SourceColumnNamesQueryPart = string.Join(", ", x.ClientDataInfos.Select(y => NpgsqlHelper.GetQualifiedName(y.TempAliasedColumnName))),
                    Returning = string.Join(", ", x.ReturningInfos.Select(y => NpgsqlHelper.GetQualifiedName(y.ColumnInfo.ColumnName))),
                    ReturningSetQueryPart = string.Join(", ", others.Select(y => $"{NpgsqlHelper.GetQualifiedName(y.My.TempAliasedColumnName)} " +
                        $" = source.{NpgsqlHelper.GetQualifiedName(y.Others.ColumnInfo.ColumnName)}"))
                };
            }).ToList();
        }

        private EntityInfo CreateEntityInfo<T>(
            string tableName,
            string tableNameQualified,
            List<MappingInfo> mappingInfo,
            NpgsqlBulkCodeBuilder<T> codeBuilderOuter = null)
        {
            var codeBuilder = codeBuilderOuter ?? new NpgsqlBulkCodeBuilder<T>();

            var info = new EntityInfo()
            {
                TableNameQualified = tableNameQualified,
                TableName = tableName,
                CodeBuilder = codeBuilder,
                MappingInfos = mappingInfo,
                PropToMappingInfo = mappingInfo.Where(x => x.Property != null).ToDictionary(x => x.TableNameQualified + "." + x.Property.Name),
                TableNames = mappingInfo.Select(x => x.TableName).Distinct().ToArray(),
                InsertClientDataInfos = mappingInfo.Where(x => x.DoInsert).ToArray(),
                UpdateClientDataWithKeysInfos = mappingInfo.Where(x => x.DoUpdate || x.IsKey).ToArray(),
                MaxIsOptionalFlag = mappingInfo.Max(x => x.IsSpecifiedFlag)
            };

#if EFCore
            info.PropertyToGenerators = mappingInfo
                .Where(x => x.LocalGenerator != null)
                .ToDictionary(x => x.DbProperty, x => x.LocalGenerator);
            info.PropertyNameToGenerators = info.PropertyToGenerators.ToDictionary(x => x.Key.Name, x => x.Value);
#endif

            var tableNames = mappingInfo.Select(x => x.TableNameQualified).Distinct().ToList();

            info.InsertQueryParts = new ConcurrentDictionary<long, List<InsertQueryParts>>();
            info.InsertQueryParts[0] = GetInsertQueryParts(mappingInfo, 0);

            info.SelectSourceForInsertQuery = "SELECT " +
                string.Join(", ", info.InsertClientDataInfos
                    .Select(x => $"{x.QualifiedColumnName} AS {x.TempAliasedColumnName}")) +
                " FROM " + string.Join(", ", tableNames);
            info.CopyColumnsForInsertQueryPart = string.Join(", ", info.InsertClientDataInfos
                .Select(x => x.TempAliasedColumnName));

            info.InsertDbGeneratedInfos = info.MappingInfos.Where(x => x.ReadBack && x.Property != null).ToArray();

            // Now time for updates
            var grouppedByTables = mappingInfo.GroupBy(x => x.TableName)
                .Select(x => new
                {
                    TableName = x.Key,
                    x.First().TableNameQualified,
                    KeyInfos = x.Where(y => y.IsKey).ToList(),
                    ClientDataInfos = x.Where(y => y.DoUpdate).ToList(),
                    ReturningInfos = x.Where(y => y.ReadBack).ToList()
                })
                .ToList();

            info.UpdateQueryParts = grouppedByTables.Select(x =>
            {
                var updateableInfos = x.ClientDataInfos;
                updateableInfos = updateableInfos
                    .Where(y => y.DoUpdate)
                    .Where(y => y.ModifierAttributes == null || y.ModifierAttributes.All(m => m.Modification != BulkOperationModification.IgnoreForUpdate))
                    .ToList();

                return new UpdateQueryParts()
                {
                    TableName = x.TableName,
                    TableNameQualified = x.TableNameQualified,
                    SetClause = string.Join(", ", updateableInfos.Select(y =>
                    {
                        var colName = NpgsqlHelper.GetQualifiedName(y.ColumnInfo.ColumnName);
                        return $"{colName} = source.{y.TempAliasedColumnName}";
                    })),
                    WhereClause = string.Join(" AND ", x.KeyInfos.Select(y =>
                    {
                        var colName = NpgsqlHelper.GetQualifiedName(y.ColumnInfo.ColumnName);
                        var clause = $"{colName} = source.{y.TempAliasedColumnName}";

                        if (y.IsNullableInClr)
                        {
                            clause = $"({clause} OR ({colName} IS NULL AND source.{y.TempAliasedColumnName} IS NULL))";
                        }

                        return clause;
                    }))
                };
            })
            .Where(x => !string.IsNullOrEmpty(x.SetClause))
            .ToList();

            info.SelectSourceForUpdateQuery = "SELECT " +
                string.Join(", ", info.UpdateClientDataWithKeysInfos
                    .Select(x => $"{x.QualifiedColumnName} AS {x.TempAliasedColumnName}")) +
                " FROM " + string.Join(", ", grouppedByTables.Select(x => x.TableNameQualified));
            info.CopyColumnsForUpdateQueryPart = string.Join(", ", info.UpdateClientDataWithKeysInfos
                .Select(x => x.TempAliasedColumnName));

            info.UpdateDbGeneratedInfos = info.MappingInfos.Where(x => x.ReadBack && x.Property != null).ToArray();

            // Rest info
            info.KeyInfos = info.MappingInfos.Where(x => x.IsKey).ToArray();

            info.KeyColumnNames = info.KeyInfos.Select(x => x.ColumnInfo.ColumnName).ToArray();

            if (codeBuilderOuter == null)
                codeBuilder.InitBuilder(info, false, ReadValue);

            return info;
        }

        /// <summary>
        /// Get unique object name using user-defined prefix.
        /// </summary>
        /// <param name="prefix">Prefix.</param>
        /// <returns>Unique name.</returns>
        internal static string GetUniqueName(string prefix)
        {
            var counter = Interlocked.Increment(ref tablesCounter);
            return $"{prefix}_{uniqueTablePrefix}_{counter}";
        }
    }
}

