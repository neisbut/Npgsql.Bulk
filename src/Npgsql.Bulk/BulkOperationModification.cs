using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk
{
    public enum BulkOperationModification
    {
        None = 0,
        
        /// <summary>
        /// Means that field will not be supplied for update
        /// </summary>
        IgnoreForUpdate = 1
    }
}
