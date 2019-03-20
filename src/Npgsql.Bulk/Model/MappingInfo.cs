using System.Reflection;
using NpgsqlTypes;
using System.Linq;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System;
#if NETSTANDARD1_5 || NETSTANDARD2_0
using Microsoft.EntityFrameworkCore.ValueGeneration;
#endif

namespace Npgsql.Bulk.Model
{
    internal class MappingInfo
    {
        PropertyInfo _property;

        public bool IsInheritanceUsed { get; set; }

        public string TableName { get; set; }

        public string TableNameQualified { get; set; }

        public string TempAliasedColumnName { get; internal set; }

        public string QualifiedColumnName { get; internal set; }

        public PropertyInfo Property
        {
            get
            {
                return _property;
            }
            set
            {
                _property = value;

                if (_property.GetCustomAttributes<RequiredAttribute>().Any())
                {
                    IsNullableInClr = false;
                }
                else if (Nullable.GetUnderlyingType(_property.PropertyType) != null)
                {
                    IsNullableInClr = true;
                }
                else if (_property.PropertyType.IsClass)
                {
                    IsNullableInClr = true;
                }
                else
                {
                    IsNullableInClr = false;
                }
            }
        }

        public MethodInfo OverrideSourceMethod { get; set; }

        public ColumnInfo ColumnInfo { get; set; }

        public NpgsqlDbType NpgsqlType { get; set; }

        public bool IsDbGenerated { get; set; }

#if NETSTANDARD1_5 || NETSTANDARD2_0
        public ValueGenerator LocalGenerator { get; set; }
#endif

        public bool IsKey { get; set; }

        public List<BulkOperationModifierAttribute> ModifierAttributes { get; set; }

        public bool IsNullableInClr { get; private set; }
    }
}
