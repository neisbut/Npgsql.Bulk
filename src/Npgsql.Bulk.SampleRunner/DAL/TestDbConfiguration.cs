using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk.SampleRunner.DAL
{
    public class TestDbConfiguration : DbConfiguration
    {
        public TestDbConfiguration()
        {
            AddInterceptor(new BulkSelectInterceptor());
        }
    }
}
