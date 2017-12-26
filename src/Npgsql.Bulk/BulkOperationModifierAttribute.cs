using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class BulkOperationModifierAttribute : Attribute
    {
        public BulkOperationModification Modification { get; private set; }

        public BulkOperationModifierAttribute(BulkOperationModification modification)
        {
            this.Modification = modification;
        }
    }
}
