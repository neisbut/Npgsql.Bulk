using Npgsql.Bulk.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Npgsql.Bulk
{
    public static class BulkQueryableExtension
    {

        public static List<T> BulkSelect<T, TKey>(
            this IQueryable<T> source,
            Expression<Func<T, TKey>> keyExpression,
            IEnumerable<TKey> keyData)
        {
            EnsureNoNavigationProperties(keyExpression);

            BulkSelectInterceptor.StartInterception();

            var keyDataTable = $"_schema_{DateTime.Now.Ticks}";
            var schemaQuery = source.Select(keyExpression);
            var schemaSql = $"CREATE TEMP TABLE {keyDataTable} ON COMMIT DROP AS ({schemaQuery} LIMIT 0)";

            var context = NpgsqlHelper.GetContextFromQuery(source);
            var conn = NpgsqlHelper.GetNpgsqlConnection(context);


            var localTr = NpgsqlHelper.EnsureOrStartTransaction(context, IsolationLevel.ReadCommitted);
            try
            {
                context.Database.ExecuteSqlCommand(schemaSql);
                var columnsInfo = NpgsqlHelper.GetColumnsInfo(context, keyDataTable);

                var propsMap = GetPropertiesMap(
                    ((IObjectContextAdapter)context).ObjectContext,
                    schemaQuery.Expression,
                    typeof(TKey));

                var mapsInfo = new List<MappingInfo>();
                foreach (var propMap in propsMap)
                {
                    var cinfo = columnsInfo[propMap.Item2];
                    mapsInfo.Add(new MappingInfo()
                    {
                        Property = propMap.Item1,
                        ColumnInfo = cinfo,
                        NpgsqlType = NpgsqlBulkUploader.GetNpgsqlType(cinfo)
                    });
                }

                var columnsCsv = string.Join(", ",
                    mapsInfo.Select(x => NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName)));

                var copySql = $"COPY {keyDataTable} ({columnsCsv}) FROM STDIN (FORMAT BINARY)";
                using (var importer = conn.BeginBinaryImport(copySql))
                {
                    foreach (var kd in keyData)
                    {
                        importer.StartRow();
                        foreach (var kp in mapsInfo)
                        {
                            importer.Write(kp.Property.GetValue(kd), kp.NpgsqlType);
                        }
                    }
                }

                var whereSql = string.Join(" AND ",
                    mapsInfo.Select(x => $"source.{NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName)}" +
                    $" = {keyDataTable}.{NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName)}"));

                var selectSql = $"SELECT source.* FROM ({source}) as source\n" +
                    $"JOIN {keyDataTable} ON {whereSql}";

                BulkSelectInterceptor.SetReplaceQuery(source.ToString(), selectSql);

                var result = source.ToList();

                localTr?.Commit();

                return result;
            }
            catch
            {
                localTr?.Rollback();
                throw;
            }
            finally
            {
                BulkSelectInterceptor.StopInterception();
            }

        }

        private static List<Tuple<PropertyInfo, int>> GetPropertiesMap(
            IObjectContextAdapter context,
            Expression expression,
            Type type)
        {
            // Thanks to https://weblogs.asp.net/dixin/entity-framework-and-linq-to-entities-5-query-translation

            ObjectContext objectContext = context.ObjectContext;

            Assembly entityFrameworkAssembly = typeof(DbContext).Assembly;
            Type funcletizerType = entityFrameworkAssembly.GetType(
                "System.Data.Entity.Core.Objects.ELinq.Funcletizer");
            MethodInfo createQueryFuncletizerMethod = funcletizerType.GetMethod(
                "CreateQueryFuncletizer", BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.InvokeMethod);
            Type expressionConverterType = entityFrameworkAssembly.GetType(
                "System.Data.Entity.Core.Objects.ELinq.ExpressionConverter");
            ConstructorInfo expressionConverterConstructor = expressionConverterType.GetConstructor(
                BindingFlags.NonPublic | BindingFlags.Instance,
                null,
                new Type[] { funcletizerType, typeof(Expression) },
                null);
            MethodInfo convertMethod = expressionConverterType.GetMethod(
                "Convert", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.InvokeMethod);
            object funcletizer = createQueryFuncletizerMethod.Invoke(null, new object[] { objectContext });
            object expressionConverter = expressionConverterConstructor.Invoke(
                new object[] { funcletizer, expression });
            DbExpression dbExpression = (DbExpression)convertMethod.Invoke(expressionConverter, new object[0]);
            DbQueryCommandTree commandTree = objectContext.MetadataWorkspace.CreateQueryCommandTree(dbExpression);
            Type planCompilerType = entityFrameworkAssembly.GetType(
                "System.Data.Entity.Core.Query.PlanCompiler.PlanCompiler");
            MethodInfo compileMethod = planCompilerType.GetMethod(
                "Compile", BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.InvokeMethod);
            object[] arguments = new object[] { commandTree, null, null, null, null };
            compileMethod.Invoke(null, arguments);

            var columnsMap = arguments[2];
            var elementsProp = columnsMap.GetType().GetProperty(
                "Element", BindingFlags.Instance | BindingFlags.NonPublic);
            var elements = elementsProp.GetValue(columnsMap);
            var elementPropsProp = elements.GetType().GetProperty(
                "Properties", BindingFlags.Instance | BindingFlags.NonPublic);
            var propsMap = ((IEnumerable)elementPropsProp.GetValue(elements)).Cast<Object>().ToList();

            var list = new List<Tuple<PropertyInfo, int>>();
            var propElType = propsMap.First().GetType();
            var nameProp = propElType.GetProperty(
                "Name", BindingFlags.Instance | BindingFlags.NonPublic);
            var colPos = propElType.GetProperty(
                "ColumnPos", BindingFlags.Instance | BindingFlags.NonPublic);

            foreach (var prop in propsMap)
            {
                list.Add(
                    Tuple.Create(
                        type.GetProperty(nameProp.GetValue(prop).ToString()),
                        (int)colPos.GetValue(prop)));
            }

            return list;
        }

        private static void EnsureNoNavigationProperties(LambdaExpression expression)
        {
            var body = expression.Body;
            if (body is NewExpression newExp)
            {
                foreach (var arg in newExp.Arguments)
                {
                    if (arg is MemberExpression memberExp)
                    {
                        if (memberExp.Expression.NodeType != ExpressionType.Parameter)
                        {
                            throw new NotSupportedException(
                                $"Navigation properties are not supported: {memberExp.Expression}");
                        }
                    }
                    else
                    {
                        throw new NotSupportedException($"Not supported: {arg.NodeType} in constructor");
                    }
                }
                return;
            }

            throw new NotSupportedException("This expression type is not supported");
        }

    }
}
