using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using Npgsql.Bulk.Model;

namespace Npgsql.Bulk
{
    /// <summary>
    /// Dyncamic code builder
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class NpgsqlBulkCodeBuilder<T>
    {
        private AssemblyName assemblyName;
        private AssemblyBuilder assemblyBuilder;
        private ModuleBuilder moduleBuilder;
        private TypeBuilder typeBuilder;
        private Type generatedType;

        public bool IsInitialized { get; private set; }

        public Action<T, NpgsqlBinaryImporter> ClientDataWriterAction { get; private set; }
        public Action<T, NpgsqlBinaryImporter> ClientDataWithKeyWriterAction { get; private set; }
        public Action<T, NpgsqlDataReader> IdentityValuesWriterAction { get; private set; }

        public void InitBuilder(MappingInfo[] clientDataInfos,
            MappingInfo[] clientDataWithKeyInfos,
            MappingInfo[] identityMappingInfos,
            Func<Type, NpgsqlDataReader, string, object> readerFunc)
        {
            var myDomain = AppDomain.CurrentDomain;
            var name = $"{typeof(T).Name}_{DateTime.Now.Ticks}";
            assemblyName = new AssemblyName { Name = name };

            assemblyBuilder = myDomain.DefineDynamicAssembly(
                assemblyName, AssemblyBuilderAccess.Run);

            moduleBuilder = assemblyBuilder.DefineDynamicModule(name);

            typeBuilder = moduleBuilder.DefineType(name, TypeAttributes.Public);

            GenerateWriteCode(clientDataInfos, clientDataWithKeyInfos, identityMappingInfos, readerFunc);

        }

        private void GenerateWriteCode(MappingInfo[] clientDataInfos,
            MappingInfo[] clientDataWithKeyInfos,
            MappingInfo[] identityMappingInfos,
            Func<Type, NpgsqlDataReader, string, object> readerFunc)
        {
            CreateWriterMethod("ClientDataWriter", clientDataInfos);
            CreateWriterMethod("ClientDataWithKeyWriter", clientDataWithKeyInfos);
            CreateReaderMethod("IdentityValuesWriter", identityMappingInfos, readerFunc);

            generatedType = typeBuilder.CreateType();

            ClientDataWriterAction = (Action<T, NpgsqlBinaryImporter>)generatedType.GetMethod("ClientDataWriter")
                .CreateDelegate(typeof(Action<T, NpgsqlBinaryImporter>));
            ClientDataWithKeyWriterAction = (Action<T, NpgsqlBinaryImporter>)generatedType.GetMethod("ClientDataWithKeyWriter")
                .CreateDelegate(typeof(Action<T, NpgsqlBinaryImporter>));
            IdentityValuesWriterAction = (Action<T, NpgsqlDataReader>)generatedType.GetMethod("IdentityValuesWriter")
                .CreateDelegate(typeof(Action<T, NpgsqlDataReader>));

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
            var writeMethod = typeof(NpgsqlBinaryImporter).GetMethods()
                .Where((x) => x.Name == "Write")
                .OrderByDescending(x => x.GetParameters().Length)
                .First();

            foreach (var info in infos)
            {
                ilOut.Emit(OpCodes.Ldarg_1);
                ilOut.Emit(OpCodes.Ldarg_0);
                ilOut.Emit(OpCodes.Call, info.Property.GetGetMethod());
                ilOut.Emit(OpCodes.Ldc_I4_S, (long)info.NpgsqlType);
                ilOut.Emit(OpCodes.Call, writeMethod.MakeGenericMethod(info.Property.PropertyType));
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
                ilOut.Emit(OpCodes.Call, readerFunc.Method);
                ilOut.Emit(OpCodes.Unbox_Any, info.Property.PropertyType);
                ilOut.Emit(OpCodes.Callvirt, info.Property.GetSetMethod());
            }

            ilOut.Emit(OpCodes.Ret);
        }

    }
}
