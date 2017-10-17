using System.Reflection;
using NpgsqlTypes;
using System;

namespace Npgsql.Bulk.Model
{
    internal class MappingInfo
    {
        public PropertyInfo Property { get; set; }

        public MethodInfo OverrideSourceMethod { get; set; }

        public ColumnInfo ColumnInfo { get; set; }

        public NpgsqlDbType NpgsqlType { get; set; }

        public bool IsDbGenerated { get; set; }

        public bool IsKey { get; set; }
    }
}
