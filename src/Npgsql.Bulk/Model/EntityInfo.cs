#if NETSTANDARD1_5 || NETSTANDARD2_0
using Microsoft.EntityFrameworkCore.ValueGeneration;
#endif
using System.Collections.Generic;
using System.Reflection;

namespace Npgsql.Bulk.Model
{
    internal class EntityInfo
    {
        public string TableName { get; set; }

        public string TableNameQualified { get; set; }

        public string[] TableNames { get; internal set; }

        public List<MappingInfo> MappingInfos { get; set; }

        public Dictionary<PropertyInfo, MappingInfo> PropToMappingInfo { get; set; }

        public string SelectSourceForInsertQuery { get; set; }

        public string CopyColumnsForInsertQueryPart { get; set; }

        public string SelectSourceForUpdateQuery { get; set; }

        public string CopyColumnsForUpdateQueryPart { get; set; }

        public List<InsertQueryParts> InsertQueryParts { get; set; }

        public List<UpdateQueryParts> UpdateQueryParts { get; set; }

        public MappingInfo[] DbGeneratedInfos { get; set; }

        public MappingInfo[] ClientDataInfos { get; set; }

        public MappingInfo[] ClientDataWithKeysInfos { get; set; }

        public MappingInfo[] KeyInfos { get; set; }

        public string ClientDataColumnNames { get; set; }

        public string ClientDataWithKeysColumnNames { get; set; }

        public string DbGeneratedColumnNames { get; set; }

        public string[] KeyColumnNames { get; set; }

        public object CodeBuilder { get; set; }

#if NETSTANDARD1_5 || NETSTANDARD2_0
        public Dictionary<PropertyInfo, ValueGenerator> PropertyToGenerators;

        public Dictionary<string, ValueGenerator> PropertyNameToGenerators;
#endif
    }
}
