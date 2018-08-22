using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk.Model
{
    public enum TableLockLevel
    {

        NoLock = 0,

        ShareRowExclusive = 1,

        Exclusive = 2,

        AccessExclusive = 3,

    }
}
