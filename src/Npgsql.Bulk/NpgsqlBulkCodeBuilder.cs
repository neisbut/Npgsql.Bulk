using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
#if NETSTANDARD1_5 || NETSTANDARD2_0
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ValueGeneration;
#endif
using Npgsql.Bulk.Model;

namespace Npgsql.Bulk
{
    /// <summary>
    /// Dyncamic code builder
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class NpgsqlBulkCodeBuilder<T>
    {

        static MethodInfo writeMethodFull = typeof(NpgsqlBinaryImporter).GetMethods()
            .Where((x) => x.Name == "Write")
            .First(x => x.GetParameters().Length == 2 &&
                x.GetParameters()[1].ParameterType == typeof(NpgsqlTypes.NpgsqlDbType));

        static MethodInfo writeMethodShort = typeof(NpgsqlBinaryImporter).GetMethods()
            .Where((x) => x.Name == "Write")
            .First(x => x.GetParameters().Length == 1);

        static MethodInfo writeNull = typeof(NpgsqlBinaryImporter).GetMethods()
            .First((x) => x.Name == "WriteNull");
        static MethodInfo toArraymethod = typeof(Enumerable).GetMethod("ToArray");

        private AssemblyName assemblyName;
        private AssemblyBuilder assemblyBuilder;
        private ModuleBuilder moduleBuilder;
        private TypeBuilder typeBuilder;
        private Type generatedType;

        public bool IsInitialized { get; private set; }

        public Action<T, NpgsqlBinaryImporter> ClientDataWriterAction { get; private set; }
        public Action<T, NpgsqlBinaryImporter> ClientDataWithKeyWriterAction { get; private set; }
        public Dictionary<string, Action<T, NpgsqlDataReader>> IdentityValuesWriterActions { get; private set; }

#if NETSTANDARD1_5 || NETSTANDARD2_0
        public Action<T, EntityEntry, Dictionary<string, ValueGenerator>> AutoGenerateValues { get; private set; }
#endif

        public void InitBuilder(
            EntityInfo entityInfo,
            Func<Type, NpgsqlDataReader, string, object> readerFunc)
        {
            var name = $"{typeof(T).Name}_{DateTime.Now.Ticks}";
            assemblyName = new AssemblyName { Name = name };

            assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(
                assemblyName, AssemblyBuilderAccess.Run);

            moduleBuilder = assemblyBuilder.DefineDynamicModule(name);

            typeBuilder = moduleBuilder.DefineType(name, TypeAttributes.Public);

            GenerateWriteCode(entityInfo, readerFunc);

        }

        private void GenerateWriteCode(
            EntityInfo entityInfo,
            Func<Type, NpgsqlDataReader, string, object> readerFunc)
        {
            MappingInfo[] clientDataInfos = entityInfo.ClientDataInfos;
            MappingInfo[] clientDataWithKeyInfos = entityInfo.ClientDataWithKeysInfos;
            MappingInfo[] identityMappingInfos = entityInfo.DbGeneratedInfos;

            var identByTableName = identityMappingInfos.GroupBy(x => x.TableName).ToList();

            CreateWriterMethod("ClientDataWriter", clientDataInfos);
            CreateWriterMethod("ClientDataWithKeyWriter", clientDataWithKeyInfos);
            

            foreach (var byTableName in identByTableName)
                CreateReaderMethod($"IdentityValuesWriter_{byTableName.Key}", byTableName.ToArray(), readerFunc);


#if NETSTANDARD1_5 || NETSTANDARD2_0
            CreateAutoGenerateMethods("AutoGenerateValues", entityInfo.PropertyToGenerators);
            generatedType = typeBuilder.CreateTypeInfo().AsType();
#else
            generatedType = typeBuilder.CreateType();
#endif

            ClientDataWriterAction = (Action<T, NpgsqlBinaryImporter>)generatedType.GetMethod("ClientDataWriter")
                .CreateDelegate(typeof(Action<T, NpgsqlBinaryImporter>));
            ClientDataWithKeyWriterAction = (Action<T, NpgsqlBinaryImporter>)generatedType.GetMethod("ClientDataWithKeyWriter")
                .CreateDelegate(typeof(Action<T, NpgsqlBinaryImporter>));
            IdentityValuesWriterActions = new Dictionary<string, Action<T, NpgsqlDataReader>>();

            foreach (var byTableName in identByTableName)
                IdentityValuesWriterActions[byTableName.Key] =
                    (Action<T, NpgsqlDataReader>)generatedType.GetMethod($"IdentityValuesWriter_{byTableName.Key}")
                        .CreateDelegate(typeof(Action<T, NpgsqlDataReader>));

#if NETSTANDARD1_5 || NETSTANDARD2_0
            AutoGenerateValues = (Action<T, EntityEntry, Dictionary<string, ValueGenerator>>)generatedType.GetMethod("AutoGenerateValues")
                .CreateDelegate(typeof(Action<T, EntityEntry, Dictionary<string, ValueGenerator>>));
#endif

            IsInitialized = true;
        }

        private void CreateWriterMethod(string methodName, MappingInfo[] infos)
        {
            var methodBuilder = typeBuilder.DefineMethod(
                methodName,
                MethodAttributes.Public | MethodAttributes.Static,
                typeof(void),
                new[] { typeof(T), typeof(NpgsqlBinaryImporter) });

            var ilOut = methodBuilder.GetILGenerator();
            var localVars = new Dictionary<Type, LocalBuilder>();

            foreach (var info in infos)
            {
                var mi = (info.OverrideSourceMethod) ?? info.Property.GetGetMethod();
                if (mi == null) throw new InvalidOperationException($"Property {info.Property.Name} is not accessible for bulk write");
                var underlying = Nullable.GetUnderlyingType(mi.ReturnType);

                if (underlying == null)
                {
                    ilOut.Emit(OpCodes.Ldarg_1);
                    ilOut.Emit(OpCodes.Ldarg_0);
                    ilOut.Emit(mi.IsVirtual ? OpCodes.Callvirt : OpCodes.Call, mi);

                    if (info.NpgsqlType == NpgsqlTypes.NpgsqlDbType.Array)
                    {
                        if (mi.ReturnType.IsArray)
                        {
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(mi.ReturnType));
                        }
                        else if (mi.ReturnType.IsGenericType && mi.ReturnType.GetGenericTypeDefinition() == typeof(List<>))
                        {
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(mi.ReturnType));
                        }
                        else
                        {
                            var toArrayLocal = toArraymethod.MakeGenericMethod(mi.ReturnType.GetGenericArguments()[0]);
                            ilOut.Emit(OpCodes.Call, toArrayLocal);
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(toArrayLocal.ReturnType));
                        }
                    }
                    else if (info.NpgsqlType == NpgsqlTypes.NpgsqlDbType.Range)
                    {
                        ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(mi.ReturnType));
                    }
                    else if (mi.ReturnType.GetTypeInfo().IsEnum)
                    {
                        ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                        ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(Enum.GetUnderlyingType(mi.ReturnType.GetTypeInfo())));
                    }
                    else
                    {
                        ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                        ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(mi.ReturnType));
                    }
                }
                else
                {
                    if (!localVars.TryGetValue(mi.ReturnType, out LocalBuilder localVar))
                        localVars[mi.ReturnType] = localVar = ilOut.DeclareLocal(mi.ReturnType);

                    ilOut.Emit(OpCodes.Ldarg_0);
                    ilOut.Emit(OpCodes.Callvirt, mi);
                    ilOut.Emit(OpCodes.Stloc, localVar);

                    ilOut.Emit(OpCodes.Ldloca_S, localVar);
                    ilOut.Emit(OpCodes.Call, mi.ReturnType.GetMethod("get_HasValue"));

                    var falseLbl = ilOut.DefineLabel();
                    var outLbl = ilOut.DefineLabel();

                    ilOut.Emit(OpCodes.Brfalse_S, falseLbl);

                    ilOut.Emit(OpCodes.Ldarg_1);
                    ilOut.Emit(OpCodes.Ldloca, localVar);
                    ilOut.Emit(OpCodes.Call, mi.ReturnType.GetMethod("get_Value"));

                    if (underlying.IsEnum)
                    {
                        ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                        ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(Enum.GetUnderlyingType(underlying)));
                    }
                    else if (info.NpgsqlType == NpgsqlTypes.NpgsqlDbType.Range)
                    {
                        ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(underlying));
                    }
                    else
                    {
                        ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                        ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(underlying));
                    }

                    ilOut.Emit(OpCodes.Br_S, outLbl);

                    ilOut.MarkLabel(falseLbl);
                    ilOut.Emit(OpCodes.Ldarg_1);
                    ilOut.Emit(OpCodes.Callvirt, writeNull);

                    ilOut.MarkLabel(outLbl);
                }
            }

            ilOut.Emit(OpCodes.Ret);
        }

        private void CreateReaderMethod(string methodName,
            MappingInfo[] infos,
            Func<Type, NpgsqlDataReader, string, object> readerFunc)
        {
            var methodBuilder = typeBuilder.DefineMethod(
                methodName,
                MethodAttributes.Public | MethodAttributes.Static,
                typeof(void),
                new[] { typeof(T), typeof(NpgsqlDataReader) });

            var ilOut = methodBuilder.GetILGenerator();

            var getTypeFromHandle = typeof(Type).GetMethod("GetTypeFromHandle");

            foreach (var info in infos)
            {
                ilOut.Emit(OpCodes.Ldarg_0);
                ilOut.Emit(OpCodes.Ldtoken, info.Property.PropertyType);
                ilOut.Emit(OpCodes.Call, getTypeFromHandle);
                ilOut.Emit(OpCodes.Ldarg_1);
                ilOut.Emit(OpCodes.Ldstr, info.ColumnInfo.ColumnName);
                ilOut.Emit(OpCodes.Call, readerFunc.GetMethodInfo());
                ilOut.Emit(OpCodes.Unbox_Any, info.Property.PropertyType);
                ilOut.Emit(OpCodes.Callvirt, info.Property.GetSetMethod());
            }

            ilOut.Emit(OpCodes.Ret);
        }


#if NETSTANDARD1_5 || NETSTANDARD2_0
        private void CreateAutoGenerateMethods(string methodName, Dictionary<PropertyInfo, ValueGenerator> generators)
        {
            var methodBuilder = typeBuilder.DefineMethod(
                methodName,
                MethodAttributes.Public | MethodAttributes.Static,
                typeof(void),
                new[] { typeof(T), typeof(EntityEntry), typeof(Dictionary<string, ValueGenerator>) });

            var ilOut = methodBuilder.GetILGenerator();
            var getGeneratorMethod = typeof(Dictionary<string, ValueGenerator>).GetProperty("Item").GetGetMethod();
            var nextMethod = typeof(ValueGenerator).GetMethod("Next");

            foreach (var generator in generators)
            {
                ilOut.Emit(OpCodes.Ldarg_0);

                ilOut.Emit(OpCodes.Ldarg_2);
                ilOut.Emit(OpCodes.Ldstr, generator.Key.Name);
                ilOut.Emit(OpCodes.Callvirt, getGeneratorMethod);

                ilOut.Emit(OpCodes.Ldarg_1);
                ilOut.Emit(OpCodes.Callvirt, nextMethod);

                ilOut.Emit(OpCodes.Unbox_Any, generator.Key.PropertyType);
                ilOut.Emit(OpCodes.Callvirt, generator.Key.GetSetMethod());
            }

            ilOut.Emit(OpCodes.Ret);
        }
#endif

    }
}
