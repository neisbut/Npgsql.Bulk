using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
#if EFCore
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
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

#if EFCore
        public Action<T, EntityEntry, Dictionary<string, ValueGenerator>> AutoGenerateValues { get; private set; }

        FieldBuilder convertersListField;

        List<ValueConverter> converters = new List<ValueConverter>();
#endif

        public void InitBuilder(
            EntityInfo entityInfo,
            Func<Type, NpgsqlDataReader, string, object> readerFunc)
        {
            var name = NpgsqlBulkUploader.GetUniqueName(typeof(T).Name);
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


#if EFCore
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

#if EFCore
            AutoGenerateValues = (Action<T, EntityEntry, Dictionary<string, ValueGenerator>>)generatedType.GetMethod("AutoGenerateValues")
                .CreateDelegate(typeof(Action<T, EntityEntry, Dictionary<string, ValueGenerator>>));

            generatedType.GetField("Converters")?.SetValue(null, converters.Select(x => x.ConvertToProvider).ToArray());
#endif

            IsInitialized = true;
        }

        private void WriteValueGet(ILGenerator ilOut, MappingInfo info, MethodInfo getValueMethod)
        {
            
#if EFCore
            if (info.ValueConverter != null)
            {
                convertersListField = convertersListField ?? 
                    typeBuilder.DefineField("Converters", typeof(Func<object, object>[]),
                        FieldAttributes.Public | FieldAttributes.Static);

                int convIndex = converters.IndexOf(info.ValueConverter);
                if (convIndex < 0)
                {
                    converters.Add(info.ValueConverter);
                    convIndex = converters.Count - 1;
                }

                ilOut.Emit(OpCodes.Ldsfld, convertersListField);
                ilOut.Emit(OpCodes.Ldc_I4_S, convIndex);
                ilOut.Emit(OpCodes.Ldelem_Ref);

                ilOut.Emit(OpCodes.Ldarg_0);
                ilOut.Emit(getValueMethod.IsVirtual ? OpCodes.Callvirt : OpCodes.Call, getValueMethod);

                ilOut.Emit(OpCodes.Callvirt, typeof(Func<object, object>).GetMethod("Invoke"));

                if (info.ValueConverter.ProviderClrType.IsValueType)
                    ilOut.Emit(OpCodes.Unbox_Any, info.ValueConverter.ProviderClrType);
                else
                    ilOut.Emit(OpCodes.Castclass, info.ValueConverter.ProviderClrType);

                return;
            }
#endif
            ilOut.Emit(OpCodes.Ldarg_0);
            ilOut.Emit(getValueMethod.IsVirtual ? OpCodes.Callvirt : OpCodes.Call, getValueMethod);
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

                var fieldType = mi.ReturnType;
                var underlying = Nullable.GetUnderlyingType(fieldType);

#if EFCore
                if (info.ValueConverter != null)
                {
                    fieldType = info.ValueConverter.ProviderClrType;
                    underlying = Nullable.GetUnderlyingType(fieldType);
                }
#endif

                if (underlying == null)
                {
                    ilOut.Emit(OpCodes.Ldarg_1);
                    WriteValueGet(ilOut, info, mi);

                    if (info.NpgsqlType == NpgsqlTypes.NpgsqlDbType.Array)
                    {
                        if (fieldType.IsArray)
                        {
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(fieldType));
                        }
                        else if (fieldType.IsGenericType && fieldType.GetGenericTypeDefinition() == typeof(List<>))
                        {
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(fieldType));
                        }
                        else
                        {
                            var toArrayLocal = toArraymethod.MakeGenericMethod(fieldType.GetGenericArguments()[0]);
                            ilOut.Emit(OpCodes.Call, toArrayLocal);
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(toArrayLocal.ReturnType));
                        }
                    }
                    else if (info.NpgsqlType == NpgsqlTypes.NpgsqlDbType.Range)
                    {
                        ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(fieldType));
                    }
                    else if (fieldType.GetTypeInfo().IsEnum)
                    {
                        ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                        ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(Enum.GetUnderlyingType(fieldType.GetTypeInfo())));
                    }
                    else
                    {
                        ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                        ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(fieldType));
                    }
                }
                else
                {
                    if (!localVars.TryGetValue(fieldType, out LocalBuilder localVar))
                        localVars[fieldType] = localVar = ilOut.DeclareLocal(fieldType);

                    WriteValueGet(ilOut, info, mi);

                    ilOut.Emit(OpCodes.Stloc, localVar);

                    ilOut.Emit(OpCodes.Ldloca_S, localVar);
                    ilOut.Emit(OpCodes.Call, fieldType.GetMethod("get_HasValue"));

                    var falseLbl = ilOut.DefineLabel();
                    var outLbl = ilOut.DefineLabel();

                    ilOut.Emit(OpCodes.Brfalse_S, falseLbl);

                    ilOut.Emit(OpCodes.Ldarg_1);
                    ilOut.Emit(OpCodes.Ldloca, localVar);
                    ilOut.Emit(OpCodes.Call, fieldType.GetMethod("get_Value"));

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


#if EFCore
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
