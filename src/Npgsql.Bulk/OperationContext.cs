using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#if EFCore
using Microsoft.EntityFrameworkCore;
#else
using System.Data.Entity;
#endif

namespace Npgsql.Bulk
{
    public class OperationContext
    {

        public DbContext Context;

        public bool IsImport;

        public OperationContext(DbContext context, bool isImport)
        {
            this.Context = context;
            this.IsImport = isImport;
        }

    }
}
