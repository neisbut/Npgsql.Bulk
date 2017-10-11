using System;
using System.Collections.Generic;
using System.Linq;

namespace Npgsql.Bulk.Model
{
    internal class EntityInfo
    {
        Lazy<List<MappingInfo>> mappingInfos;

        public string TableName { get; set; }

        public string TableNameQualified { get; set; }

        public Lazy<List<MappingInfo>> MappingInfos
        {
            get
            {
                return mappingInfos;
            }
            set
            {
                mappingInfos = value;

                ClientDataInfos = new Lazy<MappingInfo[]>(
                    () => MappingInfos.Value.Where(x => !x.IsDbGenerated).ToArray());

                ClientDataColumnNames = new Lazy<string>(() => string.Join(", ",
                    ClientDataInfos.Value.Select(x => NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName))));

                KeyInfos = new Lazy<MappingInfo[]>(
                    () => MappingInfos.Value.Where(x => x.IsKey).ToArray());

                KeyColumnNames = new Lazy<string[]>(
                    () => KeyInfos.Value.Select(x => x.ColumnInfo.ColumnName).ToArray());

                ClientDataWithKeysInfos = new Lazy<MappingInfo[]>(
                    () => MappingInfos.Value.Where(x => !x.IsDbGenerated || x.IsKey).ToArray());

                ClientDataWithKeysColumnNames = new Lazy<string>(() => string.Join(", ",
                    ClientDataWithKeysInfos.Value.Select(x => NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName))));

                DbGeneratedInfos = new Lazy<MappingInfo[]>(
                    () => MappingInfos.Value.Where(x => x.IsDbGenerated).ToArray());

                DbGeneratedColumnNames = new Lazy<string>(() => string.Join(", ",
                    DbGeneratedInfos.Value.Select(x => NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName))));

                SetClause = new Lazy<string>(
                    () => string.Join(", ", ClientDataInfos.Value.Select(
                        x =>
                        {
                            var colName = NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName);
                            return $"{colName} = source.{colName}";
                        })));

                WhereClause = new Lazy<string>(
                    () => string.Join(", ", KeyInfos.Value.Select(
                        x =>
                        {
                            var colName = NpgsqlHelper.GetQualifiedName(x.ColumnInfo.ColumnName);
                            return $"{TableNameQualified}.{colName} = source.{colName}";
                        })));
            }
        }

        public Lazy<MappingInfo[]> DbGeneratedInfos { get; private set; }

        public Lazy<MappingInfo[]> ClientDataInfos { get; private set; }

        public Lazy<MappingInfo[]> ClientDataWithKeysInfos { get; private set; }

        public Lazy<MappingInfo[]> KeyInfos { get; private set; }

        public Lazy<string> ClientDataColumnNames { get; private set; }

        public Lazy<string> ClientDataWithKeysColumnNames { get; private set; }

        public Lazy<string> DbGeneratedColumnNames { get; private set; }

        public Lazy<string[]> KeyColumnNames { get; private set; }

        public Lazy<string> SetClause { get; private set; }

        public Lazy<string> WhereClause { get; private set; }

        public object CodeBuilder { get; set; }
    }
}
