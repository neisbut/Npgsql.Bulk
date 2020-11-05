using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using Npgsql.Bulk.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace Npgsql.Bulk
{
    public class ValueHelper<T>
    {

        internal static Dictionary<string, MappingInfo> MappingInfos;

        public static TResult Get<TResult, TClr>(T model, string propName, OperationContext opContext, TClr localValue)
        {
            var info = MappingInfos[propName];

#pragma warning disable EF1001 // Internal EF Core API usage.

            object currentValue;

            if (!opContext.IsImport)
            {
                var sm = ((IDbContextDependencies)opContext.Context).StateManager;
                var entry = sm.GetOrCreateEntry(model);
                if (info.Property == null)
                    currentValue = entry.GetCurrentValue(info.DbProperty);
                else
                    currentValue = localValue;
            }
            else
            {
                if (info.DbProperty.IsShadowProperty())
                {
                    currentValue = info.LocalGenerator.Next(null);
                }
                else
                {
                    currentValue = localValue;
                }
            }

#pragma warning restore EF1001 // Internal EF Core API usage.

            // Convert if needed
            if (info.ValueConverter != null)
            {
                currentValue = info.ValueConverter.ConvertToProvider(currentValue);
            }

            return (TResult)currentValue;
        }

        public static long GetIsSpecifiedFlag<TField>(TField value, string propName, long specifiedFieldFlag)
        {
            if (typeof(TField) == typeof(int) || typeof(TField) == typeof(long) || typeof(TField) == typeof(byte) || typeof(TField) == typeof(Int16) ||
                typeof(TField) == typeof(uint) || typeof(TField) == typeof(ulong) || typeof(TField) == typeof(UInt16))
            {
                var info = MappingInfos[propName];
                if (info.DbProperty.IsPrimaryKey())
                {
                    var asNumber = Convert.ToInt64(value);
                    if (asNumber > 0)
                        return specifiedFieldFlag;
                    else
                        return 0;
                }
            }

            if (!EqualityComparer<TField>.Default.Equals(value, default(TField)))
            {
                return specifiedFieldFlag;
            }
            else
            {
                return 0;
            }
        }
    }
}
