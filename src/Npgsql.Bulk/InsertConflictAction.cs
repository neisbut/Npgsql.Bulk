using Npgsql.Bulk.Model;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Npgsql.Bulk
{
    public class InsertConflictAction
    {
        /// <summary>
        /// DO NOTHING conflict action
        /// </summary>
        public static InsertConflictAction DoNothing() => new InsertConflictAction() { doUpdate = false };

        /// <summary>
        /// DO NOTHING ( sql )
        /// </summary>
        /// <param name="conflictTargetSql"></param>
        /// <returns></returns>
        public static InsertConflictAction DoNothing(string conflictTargetSql) => new InsertConflictAction()
        {
            doUpdate = false,
            conflictTarget = conflictTargetSql
        };

        public static InsertConflictAction DoNothingColumn(string columnName) => DoNothing($"({columnName})");

        public static InsertConflictAction DoNothingIndex(string indexName) => DoNothing($"({indexName})");

        public static InsertConflictAction DoNothingConstraint(string constraint) => DoNothing($"ON CONSTRAINT {constraint}");

        public static InsertConflictAction Update<T>(string conflictTargetSql, params Expression<Func<T, object>>[] updateProperties) =>
            Update<T>(conflictTargetSql, updateProperties.Select(UnwrapProperty<T>).ToArray());

        public static InsertConflictAction Update<T>(string conflictTargetSql, params PropertyInfo[] updateProperties) => new InsertConflictAction
        {
            doUpdate = true,
            conflictTarget = conflictTargetSql,
            updateProperties = updateProperties
        };

        public static InsertConflictAction UpdateProperty<T>(Expression<Func<T, object>> conflictProperty, params Expression<Func<T, object>>[] updateProperties) =>
            UpdateProperty<T>(UnwrapProperty<T>(conflictProperty), updateProperties.Select(UnwrapProperty<T>).ToArray());

        public static InsertConflictAction UpdateProperty<T>(Expression<Func<T, object>> conflictProperty, params PropertyInfo[] updateProperties) =>
            UpdateProperty<T>(UnwrapProperty<T>(conflictProperty), updateProperties);

        public static InsertConflictAction UpdateProperty<T>(PropertyInfo conflictProperty, params PropertyInfo[] updateProperties) => new InsertConflictAction
        {
            doUpdate = true,
            conflictProperty = conflictProperty,
            updateProperties = updateProperties
        };

        public static InsertConflictAction UpdateColumn<T>(string columnName, params Expression<Func<T, object>>[] updateProperties) =>
            UpdateColumn<T>(columnName, updateProperties.Select(UnwrapProperty<T>).ToArray());

        public static InsertConflictAction UpdateColumn<T>(string columnName, params PropertyInfo[] updateProperties) => Update<T>($"({columnName})", updateProperties);

        public static InsertConflictAction UpdateIndex<T>(string indexName, params Expression<Func<T, object>>[] updateProperties) =>
            UpdateIndex<T>(indexName, updateProperties.Select(UnwrapProperty<T>).ToArray());

        public static InsertConflictAction UpdateIndex<T>(string indexName, params PropertyInfo[] updateProperties) => Update<T>($"({indexName})", updateProperties);

        public static InsertConflictAction UpdateConstraint<T>(string constraint, params Expression<Func<T, object>>[] updateProperties) =>
            UpdateConstraint<T>(constraint, updateProperties.Select(UnwrapProperty<T>).ToArray());

        public static InsertConflictAction UpdateConstraint<T>(string constraint, params PropertyInfo[] updateProperties) =>
            Update<T>($"ON CONSTRAINT {constraint}", updateProperties);

        bool doUpdate;
        string conflictTarget;
        PropertyInfo conflictProperty;
        PropertyInfo[] updateProperties;

        /// <summary>
        /// ctor for DO NOTHING case
        /// </summary>
        /// <param name="_"></param>
        private InsertConflictAction()
        {
        }

        internal static PropertyInfo UnwrapProperty<T>(LambdaExpression expression)
        {
            var propExpression = expression.Body;
            while (propExpression.GetType() == typeof(UnaryExpression))
                propExpression = ((UnaryExpression)propExpression).Operand;

            if (propExpression.NodeType != ExpressionType.MemberAccess)
                throw new InvalidOperationException($"MemberAccess was expected, but got: '{expression}'");
            var updateMember = ((MemberExpression)propExpression).Member;
            if (updateMember.MemberType != MemberTypes.Property)
                throw new InvalidOperationException($"Property was expected, but got: '{updateMember.MemberType}'");

            var pi = (PropertyInfo)updateMember;

            if (!updateMember.DeclaringType.IsInterface)
                return pi;
            else
            {
                var map = typeof(T).GetInterfaceMap(updateMember.DeclaringType);
                var getInInterface = pi.GetGetMethod();
                var i = Array.IndexOf(map.InterfaceMethods, getInInterface);
                var getInClass = map.TargetMethods[i];
                var pc = typeof(T).GetProperties().First(x => x.GetGetMethod() == getInClass);

                return pc;
            }
        }

        internal object GetSql(EntityInfo mapping)
        {
            string conflictAction;

            if (!doUpdate)
            {
                conflictAction = "DO NOTHING";
            }
            else
            {
                if (updateProperties == null)
                    throw new ArgumentNullException(nameof(updateProperties));
                if (updateProperties.Length == 0)
                    throw new InvalidOperationException("Update properties list update can't be empty");

                if (conflictProperty != null)
                {
                    if (!mapping.PropToMappingInfo.TryGetValue(conflictProperty.Name, out MappingInfo info))
                        throw new InvalidOperationException($"Can't find property {conflictProperty} in mapping");

                    conflictTarget = $"({NpgsqlHelper.GetQualifiedName(info.ColumnInfo.ColumnName)})";
                }

                var sb = new StringBuilder(100);
                sb.Append("DO UPDATE SET ");
                foreach (var updateProp in updateProperties)
                {
                    if (!mapping.PropToMappingInfo.TryGetValue(updateProp.Name, out MappingInfo info))
                        throw new InvalidOperationException($"Can't find property {updateProp} in mapping");

                    sb.Append(NpgsqlHelper.GetQualifiedName(info.ColumnInfo.ColumnName));
                    sb.Append(" = EXCLUDED.");
                    sb.Append(NpgsqlHelper.GetQualifiedName(info.ColumnInfo.ColumnName));
                    sb.Append(", ");
                }

                sb.Length -= 2;
                conflictAction = sb.ToString();
            }

            return $"ON CONFLICT {conflictTarget} {conflictAction}";
        }
    }
}
