using System.Collections.Concurrent;
using System.Data.Common;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure.Interception;
using System.Threading;
using System.Linq;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Npgsql.Bulk
{
    public class BulkSelectInterceptor : IDbCommandInterceptor
    {
        /// <summary>
        /// Map for ThreadId -> Replaced SQL
        /// </summary>
        internal static ConcurrentDictionary<int, string> ReplaceDictionary = new ConcurrentDictionary<int, string>();

        static volatile int refCount = 0;

        internal static void StartInterception()
        {
            Interlocked.Increment(ref refCount);
        }

        internal static void StopInterception()
        {
            Interlocked.Decrement(ref refCount);

            if (!IsInterceptionEnabled)
            {
                if (ReplaceDictionary.Any())
                    throw new InvalidOperationException("It seems BulkSelectInterceptor is not installed.");
            }
        }

        private static bool IsInterceptionEnabled
        {
            get
            {
                return refCount > 0;
            }
        }

        internal static void SetReplaceQuery(string oldQuery, string newQuery)
        {
            ReplaceDictionary[Thread.CurrentThread.ManagedThreadId] = newQuery;
        }

        public void NonQueryExecuted(DbCommand command, DbCommandInterceptionContext<int> interceptionContext)
        {
        }

        public void NonQueryExecuting(DbCommand command, DbCommandInterceptionContext<int> interceptionContext)
        {
        }

        public void ReaderExecuted(DbCommand command, DbCommandInterceptionContext<DbDataReader> interceptionContext)
        {
        }

        public void ReaderExecuting(DbCommand command, DbCommandInterceptionContext<DbDataReader> interceptionContext)
        {
            if (!IsInterceptionEnabled)
                return;

            if (ReplaceDictionary.TryRemove(Thread.CurrentThread.ManagedThreadId, out string newSql))
            {
                command.CommandText = newSql;
            }
        }

        public void ScalarExecuted(DbCommand command, DbCommandInterceptionContext<object> interceptionContext)
        {
        }

        public void ScalarExecuting(DbCommand command, DbCommandInterceptionContext<object> interceptionContext)
        {
        }

    }
}
