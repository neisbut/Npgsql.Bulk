using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
#if EFCore
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.ValueGeneration;
#else
using System.Data.Entity;
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

        public Action<T, NpgsqlBinaryImporter, DbContext> WriterForInsertAction { get; private set; }
        public Action<T, NpgsqlBinaryImporter, DbContext> WriterForUpdateAction { get; private set; }
        public Dictionary<string, Action<T, NpgsqlDataReader>> InsertIdentityValuesWriterActions { get; private set; }

        public Dictionary<string, Action<T, NpgsqlDataReader>> UpdateIdentityValuesWriterActions { get; private set; }

        public Func<T, long> ClassifyOptionals { get; private set; }

#if EFCore
        // public Action<T, EntityEntry, Dictionary<string, ValueGenerator>> AutoGenerateValues { get; private set; }



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
            CreateWriterMethod("WriterForInsertAction", entityInfo.InsertClientDataInfos);
            CreateWriterMethod("WriterForUpdateAction", entityInfo.UpdateClientDataWithKeysInfos);

            foreach (var byTableName in entityInfo.InsertDbGeneratedInfos.GroupBy(x => x.TableName))
                CreateReaderMethod($"IdentityValuesWriter_{byTableName.Key}_Insert", byTableName.ToArray(), readerFunc);
            foreach (var byTableName in entityInfo.UpdateDbGeneratedInfos.GroupBy(x => x.TableName))
                CreateReaderMethod($"IdentityValuesWriter_{byTableName.Key}_Update", byTableName.ToArray(), readerFunc);

#if EFCore
            CreateClassifyOptionals(entityInfo.InsertClientDataInfos.Union(entityInfo.UpdateClientDataWithKeysInfos).ToArray());

            //CreateAutoGenerateMethods("AutoGenerateValues", entityInfo.PropertyToGenerators);
            generatedType = typeBuilder.CreateTypeInfo().AsType();
#else
            generatedType = typeBuilder.CreateType();
#endif

            WriterForInsertAction = (Action<T, NpgsqlBinaryImporter, DbContext>)generatedType.GetMethod("WriterForInsertAction")
                .CreateDelegate(typeof(Action<T, NpgsqlBinaryImporter, DbContext>));
            WriterForUpdateAction = (Action<T, NpgsqlBinaryImporter, DbContext>)generatedType.GetMethod("WriterForUpdateAction")
                .CreateDelegate(typeof(Action<T, NpgsqlBinaryImporter, DbContext>));

            InsertIdentityValuesWriterActions = new Dictionary<string, Action<T, NpgsqlDataReader>>();
            UpdateIdentityValuesWriterActions = new Dictionary<string, Action<T, NpgsqlDataReader>>();

            foreach (var byTableName in entityInfo.InsertDbGeneratedInfos.GroupBy(x => x.TableName))
                InsertIdentityValuesWriterActions[byTableName.Key] =
                    (Action<T, NpgsqlDataReader>)generatedType.GetMethod($"IdentityValuesWriter_{byTableName.Key}_Insert")
                        .CreateDelegate(typeof(Action<T, NpgsqlDataReader>));

            foreach (var byTableName in entityInfo.UpdateDbGeneratedInfos.GroupBy(x => x.TableName))
                UpdateIdentityValuesWriterActions[byTableName.Key] =
                    (Action<T, NpgsqlDataReader>)generatedType.GetMethod($"IdentityValuesWriter_{byTableName.Key}_Update")
                        .CreateDelegate(typeof(Action<T, NpgsqlDataReader>));

#if EFCore
            generatedType.GetField("Converters")?.SetValue(null, converters.Select(x => x.ConvertToProvider).ToArray());

            ValueHelper<T>.MappingInfos = entityInfo.InsertClientDataInfos
                .Union(entityInfo.UpdateClientDataWithKeysInfos)
                .ToDictionary(x => x.QualifiedColumnName);

            ClassifyOptionals = (Func<T, long>)generatedType.GetMethod("ClassifyOptionals")
               .CreateDelegate(typeof(Func<T, long>));
#else
            ClassifyOptionals = (_) => 0;
#endif

            IsInitialized = true;
        }

#if EFCore
        private void CreateClassifyOptionals(MappingInfo[] infos)
        {
            var methodBuilder = typeBuilder.DefineMethod(
                "ClassifyOptionals",
                MethodAttributes.Public | MethodAttributes.Static,
                typeof(long),
                new[] { typeof(T) });

            var ilOut = methodBuilder.GetILGenerator();
            ilOut.Emit(OpCodes.Ldc_I8, (long)0);

            foreach (var info in infos.Where(x => x.IsSpecifiedFlag > 0))
            {
                var targetType = info.ValueConverter?.ProviderClrType ?? info.DbProperty.ClrType;

                ilOut.Emit(OpCodes.Ldarg_0);
                ilOut.Emit(OpCodes.Callvirt, info.Property.GetGetMethod());
                
                ilOut.Emit(OpCodes.Ldstr, info.QualifiedColumnName);

                ilOut.Emit(OpCodes.Ldc_I8, info.IsSpecifiedFlag);

                ilOut.Emit(OpCodes.Call, typeof(ValueHelper<T>).GetMethod(nameof(ValueHelper<T>.GetIsSpecifiedFlag))
                    .MakeGenericMethod(targetType));

                ilOut.Emit(OpCodes.Add);
            }

            ilOut.Emit(OpCodes.Ret);
        }
#endif
        private void WriteValueGet(ILGenerator ilOut, MappingInfo info, MethodInfo getValueMethod)
        {

#if EFCore
            var targetType = info.ValueConverter?.ProviderClrType ?? info.DbProperty.ClrType;

            ilOut.Emit(OpCodes.Ldarg_0);
            ilOut.Emit(OpCodes.Ldstr, info.QualifiedColumnName);
            ilOut.Emit(OpCodes.Ldarg_2);

            if (info.Property != null)
            {
                ilOut.Emit(OpCodes.Ldarg_0);
                ilOut.Emit(getValueMethod.IsVirtual ? OpCodes.Callvirt : OpCodes.Call, getValueMethod);
            }
            else
            {
                ilOut.Emit(OpCodes.Ldnull);
            }

            ilOut.Emit(OpCodes.Call, typeof(ValueHelper<T>).GetMethod(nameof(ValueHelper<T>.Get))
                .MakeGenericMethod(targetType, info.DbProperty.ClrType));

#else
            ilOut.Emit(OpCodes.Ldarg_0);
            ilOut.Emit(getValueMethod.IsVirtual ? OpCodes.Callvirt : OpCodes.Call, getValueMethod);
#endif

        }


        private void CreateWriterMethod(string methodName, MappingInfo[] infos)
        {

            var methodBuilder = typeBuilder.DefineMethod(
                methodName,
                MethodAttributes.Public | MethodAttributes.Static,
                typeof(void),
                new[] { typeof(T), typeof(NpgsqlBinaryImporter), typeof(DbContext) });

            var ilOut = methodBuilder.GetILGenerator();
            var localVars = new Dictionary<Type, LocalBuilder>();

            foreach (var info in infos)
            {
                var mi = (info.OverrideSourceMethod) ?? info.Property?.GetGetMethod();

#if !EFCore
                if (mi == null)
                    throw new InvalidOperationException($"Property {info.Property.Name} is not accessible for bulk write");

                var fieldType = mi.ReturnType;
                var underlying = Nullable.GetUnderlyingType(fieldType);
#else
                var fieldType = info.DbProperty.ClrType;
                var underlying = Nullable.GetUnderlyingType(fieldType);
#endif

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
                        // If an enum's type is not known then assume Npgsql will map it to the
                        // correct postgres enum type.  If the CLR enum wasn't already registered
                        // with Npgsql then Npgsql will throw an exception with an informative
                        // error message.
                        if (info.NpgsqlType == NpgsqlTypes.NpgsqlDbType.Unknown)
                        {
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(fieldType.GetTypeInfo()));
                        }
                        else
                        {
                            // Else, this CLR enum does not map to a postgres enum, so assume the
                            // field type in postgres matches the CLR enum's underlying type,
                            // e.g. "int".
                            ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                            ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(Enum.GetUnderlyingType(fieldType.GetTypeInfo())));
                        }
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
                        // If an enum's type is not known then assume Npgsql will map it to the
                        // correct postgres enum type.  If the CLR enum wasn't already registered
                        // with Npgsql then Npgsql will throw an exception with an informative
                        // error message.
                        if (info.NpgsqlType == NpgsqlTypes.NpgsqlDbType.Unknown)
                        {
                            ilOut.Emit(OpCodes.Callvirt, writeMethodShort.MakeGenericMethod(underlying));
                        }
                        else
                        {
                            // Else, this CLR enum does not map to a postgres enum, so assume the
                            // field type in postgres matches the CLR enum's underlying type,
                            // e.g. "int".
                            ilOut.Emit(OpCodes.Ldc_I4_S, (int)info.NpgsqlType);
                            ilOut.Emit(OpCodes.Call, writeMethodFull.MakeGenericMethod(Enum.GetUnderlyingType(underlying)));
                        }
                        
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
                ilOut.Emit(OpCodes.Callvirt, info.Property.GetSetMethod(true));
            }

            ilOut.Emit(OpCodes.Ret);
        }


#if EFCore
        //private void CreateAutoGenerateMethods(string methodName, Dictionary<IProperty, ValueGenerator> generators)
        //{
        //    var methodBuilder = typeBuilder.DefineMethod(
        //        methodName,
        //        MethodAttributes.Public | MethodAttributes.Static,
        //        typeof(void),
        //        new[] { typeof(T), typeof(EntityEntry), typeof(Dictionary<string, ValueGenerator>) });

        //    var ilOut = methodBuilder.GetILGenerator();
        //    var getGeneratorMethod = typeof(Dictionary<string, ValueGenerator>).GetProperty("Item").GetGetMethod();
        //    var nextMethod = typeof(ValueGenerator).GetMethod("Next");

        //    foreach (var generator in generators)
        //    {
        //        ilOut.Emit(OpCodes.Ldarg_0);

        //        ilOut.Emit(OpCodes.Ldarg_2);
        //        ilOut.Emit(OpCodes.Ldstr, generator.Key.Name);
        //        ilOut.Emit(OpCodes.Callvirt, getGeneratorMethod);

        //        ilOut.Emit(OpCodes.Ldarg_1);
        //        ilOut.Emit(OpCodes.Callvirt, nextMethod);

        //        ilOut.Emit(OpCodes.Unbox_Any, generator.Key.ClrType);

        //        if (generator.Key.PropertyInfo != null)
        //        {
        //            ilOut.Emit(OpCodes.Callvirt, generator.Key.PropertyInfo.GetSetMethod());
        //        }
        //        else
        //        {
        //            ilOut.Emit(OpCodes.Pop);
        //        }
        //    }

        //    ilOut.Emit(OpCodes.Ret);
        //}
#endif

    }
}
