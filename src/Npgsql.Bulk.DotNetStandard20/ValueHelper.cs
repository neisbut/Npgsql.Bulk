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

        public static TResult Get<TResult>(T model, string propName, DbContext context)
        {
            var info = MappingInfos[propName];

            if (info.Property == null)
            {
                var sm = ((IDbContextDependencies)context).StateManager;
                var entry = sm.GetOrCreateEntry(model);

                return (TResult)entry.GetCurrentValue(info.DbProperty);
            }

            return default(TResult);
        }
    }
}
