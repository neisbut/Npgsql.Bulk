#if EFCore
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.ValueGeneration;
#endif
using System.Collections.Concurrent;
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

        public Dictionary<string, MappingInfo> PropToMappingInfo { get; set; }

        public string SelectSourceForInsertQuery { get; set; }

        public string CopyColumnsForInsertQueryPart { get; set; }

        public string SelectSourceForUpdateQuery { get; set; }

        public string CopyColumnsForUpdateQueryPart { get; set; }

        public ConcurrentDictionary<long, List<InsertQueryParts>> InsertQueryParts { get; set; }

        public List<UpdateQueryParts> UpdateQueryParts { get; set; }

        public MappingInfo[] InsertDbGeneratedInfos { get; set; }

        public MappingInfo[] UpdateDbGeneratedInfos { get; set; }

        public MappingInfo[] InsertClientDataInfos { get; set; }

        public MappingInfo[] UpdateClientDataWithKeysInfos { get; set; }

        public MappingInfo[] KeyInfos { get; set; }

        public string[] KeyColumnNames { get; set; }

        public object CodeBuilder { get; set; }

        public long MaxIsOptionalFlag { get; set; }

#if EFCore
        public Dictionary<IProperty, ValueGenerator> PropertyToGenerators;

        public Dictionary<string, ValueGenerator> PropertyNameToGenerators;
#endif
    }
}
