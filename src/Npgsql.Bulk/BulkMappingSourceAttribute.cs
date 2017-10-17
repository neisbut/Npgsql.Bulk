using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk
{
    [AttributeUsage(AttributeTargets.Property)]
    public class BulkMappingSourceAttribute : Attribute
    {
        public string PropertyName { get; }

        public BulkMappingSourceAttribute(string propertyName)
        {
            PropertyName = propertyName;
        }

    }
}
