using System.Reflection;
using NpgsqlTypes;
using System.Linq;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System;
#if EFCore
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
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

#if EFCore
                if (_property == null)
                {
                    IsNullableInClr = false;
                    return;
                }
#endif

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

#if EFCore
        public ValueGenerator LocalGenerator { get; set; }

        public ValueConverter ValueConverter { get; set; }

        public IProperty DbProperty { get; internal set; }

#endif

        public bool IsKey { get; set; }

        public List<BulkOperationModifierAttribute> ModifierAttributes { get; set; }

        public bool IsNullableInClr { get; private set; }

        public bool DoUpdate { get; set; } = true;

        public bool DoInsert { get; set; } = true;

        public bool ReadBack { get; set; }

        public long IsSpecifiedFlag { get; set; }

    }
}
