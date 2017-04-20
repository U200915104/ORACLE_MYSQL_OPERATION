using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Reflection;
using System.Text;
using System.Data.SqlClient;
//using TPI;
using System;

namespace DAL
{
    public abstract class DBHelper
    {
        public string ConnectionString;
        public bool IsSlaveServer = false;

        
        private static readonly DBHelper mDBHelper = (DBHelper)Assembly.Load("DAL").CreateInstance(string.Format("DAL.{0}", ConfigurationManager.AppSettings["DBType"]), true);

        public static DBHelper GetInstance()
        {
            //DBHelper dbInstance = (DBHelper)Assembly.Load("DAL").CreateInstance(string.Format("DAL.{0}", ConfigurationSettings.AppSettings["DBType"]));
            return mDBHelper;
        }

        public static DBHelper GetInstance(string DBType)
        {
            DBHelper dbInstance = (DBHelper)Assembly.Load("DAL").CreateInstance(string.Format("DAL.{0}", DBType), true);
            return dbInstance;
        }

        public static DBHelper GetInstance(string DBType, string ConnectionSettings)
        {
            DBHelper dbInstance = (DBHelper)Assembly.Load("DAL").CreateInstance(string.Format("DAL.{0}", DBType), true, BindingFlags.CreateInstance, null, ConnectionSettings.Split(','), null, null);
            return dbInstance;
        }

        public static DBHelper GetInstance(string DBType, string ConnectionString, bool IsAppSettings)
        {
            string[] arrConn = ConnectionString.Split('$');
            object[] objParam = new object[arrConn.Length + 1];
            for (int i = 0; i < arrConn.Length; i++)
            {
                objParam[i] = arrConn[i];
            }
            objParam[arrConn.Length] = IsAppSettings;
            
            DBHelper dbInstance = (DBHelper)Assembly.Load("DAL").CreateInstance(string.Format("DAL.{0}", DBType), true, BindingFlags.CreateInstance, null, objParam, null, null);

            arrConn = null;
            objParam = null;

            return dbInstance;
        }

        public static DBHelper GetInstance(string DBType, string DataBase, string Address, string Port, string UserName, string PassWord)
        {
            string[] arrParam = new string[5];
            arrParam[0] = DataBase;
            arrParam[1] = Address;
            arrParam[2] = Port;
            arrParam[3] = UserName;
            arrParam[4] = PassWord;

            DBHelper dbInstance = (DBHelper)Assembly.Load("DAL").CreateInstance(string.Format("DAL.{0}", DBType), true, BindingFlags.CreateInstance, null, arrParam, null, null);
            return dbInstance;
        }

        public abstract bool IsConnected();

        public abstract DataTable GetDataTable(string Sql);
        //public abstract DataTable GetDataTable(string Fields, string TableName);
        //public abstract DataTable GetDataTable(string Fields, string TableName, string Condition);
        //public abstract DataTable GetDataTable(string Fields, string TableName, string Condition, string Order);
        //public abstract DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len);
        //public abstract DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len, string PreHitWord, string PostHitWord);
        //public abstract int GetCount(string TableName, string Condition);
        public abstract int ExecuteSql(string Sql);

        public virtual bool ExecuteSql(List<string> SqlList)
        {
            foreach (string strSql in SqlList)
            {
                if (ExecuteSql(strSql) < 0)
                {
                    return false;
                }
            }
            return true;
        }

        public virtual int ExecuteSql(string Sql, IDataParameter[] Param)
        {
            return 0;
        }

        public virtual int ExecuteSql(string Sql, Dictionary<string, object> Parameters)
        {
            return 0;
        }

        public virtual bool MoveData(string SrcTable, string DestTable, string Condition)
        {
            return false;
        }

        public virtual bool MoveData(string SrcTable, string DestTable, string Condition, string[] Fields, string[] Values)
        {
            return false;
        }

        public virtual DataTable GetDataTable(string Fields, string TableName)
        {
            return GetDataTable(string.Format("SELECT {0} FROM {1} ", Fields, TableName));
        }

        public virtual DataTable GetDataTable(string Fields, string TableName, string Condition)
        {
            StringBuilder sbSql = new StringBuilder();

            sbSql.AppendFormat("SELECT {0} FROM {1} ", Fields, TableName);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat("WHERE {0} ", Condition);
            }

            string strSql = sbSql.ToString();
            sbSql = null;

            return GetDataTable(strSql);
        }

        public virtual DataTable GetDataTable(string Fields, string TableName, string Condition, string Order)
        {
            StringBuilder sbSql = new StringBuilder();

            sbSql.AppendFormat("SELECT {0} FROM {1} ", Fields, TableName);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat("WHERE {0} ", Condition);
            }

            if (!string.IsNullOrEmpty(Order))
            {
                sbSql.AppendFormat(" ORDER BY {0}", Order);
            }

            string strSql = sbSql.ToString();
            sbSql = null;

            return GetDataTable(strSql);
        }

        public virtual DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len)
        {
            StringBuilder sbSql = new StringBuilder();

            sbSql.AppendFormat("SELECT {0} FROM ( SELECT ROW_NUMBER() OVER(ORDER BY {2}) as TempID, {0} FROM {1}  ", Fields, TableName, string.IsNullOrEmpty(Order) ? Key : Order);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat("WHERE {0} ", Condition);
            }
            sbSql.AppendFormat(") as TempTab where TempID between {0} and {1}", Start, Start + Len - 1);

            string strSql = sbSql.ToString();
            sbSql = null;

            return GetDataTable(strSql);
        }

        public virtual DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len, string PreHitWord, string PostHitWord)
        {
            return GetDataTable(Fields, TableName, Condition, Order, Key, Start, Len);
        }

        public virtual int GetCount(string TableName, string Condition)
        {
            StringBuilder sbSql = new StringBuilder();
            sbSql.AppendFormat("SELECT COUNT(1) FROM {0} ", TableName);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat("WHERE {0}", Condition);
            }

            DataTable dtData = GetDataTable(sbSql.ToString());

            int iRet = 0;
            if (dtData != null)
            {
                iRet = Convert.ToInt32(dtData.Rows[0][0].ToString());
                dtData.Dispose();
            }

            sbSql = null;

            return iRet;
        }

        private string CharacterHandler(string CharacterStr)
        {
            string strRet = CharacterStr.Replace("'", "''").Replace(@"\", @"\\");
            return strRet;
        }

        public virtual bool Insert(string TableName, string[] Fields, object[] Values)
        {
            StringBuilder sbFields = new StringBuilder();
            StringBuilder sbValues = new StringBuilder();
            for (int i = 0; i < Fields.Length && i < Values.Length; i++)
            {
                sbFields.AppendFormat("{0},", Fields[i]);
                if (Values[i] == null)
                {
                    sbValues.Append("NULL,");
                }
                else
                {
                    sbValues.AppendFormat("'{0}',", CharacterHandler(Values[i].ToString()));
                }
            }

            string strSql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", TableName, sbFields.ToString().Trim(','), sbValues.ToString().Trim(','));

            sbFields = null;
            sbValues = null;

            if (ExecuteSql(strSql) > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public virtual bool Update(string TableName, string Condition, string[] Fields, object[] Values)
        {
            StringBuilder sbSql = new StringBuilder();
            for (int i = 0; i < Fields.Length && i < Values.Length; i++)
            {
                if (Values[i] == null)
                {
                    sbSql.AppendFormat("{0}=NULL,", Fields[i]);
                }
                else
                {
                    sbSql.AppendFormat("{0}='{1}',", Fields[i], CharacterHandler(Values[i].ToString()));
                }
            }

            string strSql = string.Format("UPDATE {0} SET {1}", TableName, sbSql.ToString().Trim(','));
            if (!string.IsNullOrEmpty(Condition))
            {
                strSql = string.Format("{0} WHERE {1}", strSql, Condition);
            }

            sbSql = null;

            if (ExecuteSql(strSql) > -1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public virtual bool Delete(string TableName, string Condition)
        {
            StringBuilder sbSql = new StringBuilder();
            sbSql.AppendFormat("DELETE FROM {0}", TableName);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat(" WHERE {0}", Condition);
            }

            string strSql = sbSql.ToString();
            sbSql = null;

            if (ExecuteSql(strSql) >= 0)
            {
                return true;
            }
            else 
            {
                return false;
            }
        }        

        public virtual DataSet RunProcedure(string ProcName)
        {
            return null;
        }

        public virtual DataSet RunProcedure(string ProcName, IDataParameter[] Parameters)
        {
            return null;
        }

        public virtual DataSet RunProcedure(string ProcName, Dictionary<string, object> Parameters)
        {
            return null;
        }

        public virtual bool AppendDataText(string TableName, string DataText)
        {
            return false;
        }

        public virtual string RemoveSpecialCharacter(string Condition)
        {
            return string.Empty;
        }

        public virtual string FilterSpecialCharacter(string Condition)
        {
            return string.Empty;
        }

        public virtual string FilterSpecialCharacter(string Condition, string Prefix, string Postfix)
        {
            return string.Empty;
        }

        public virtual string GetSql(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len)
        {
            return string.Empty;
        }

        public virtual string GetSql(string Fields, string TableName, string Condition)
        {
            StringBuilder sbSql = new StringBuilder();

            sbSql.AppendFormat("SELECT {0} FROM {1} ", Fields, TableName);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat("WHERE {0} ", Condition);
            }

            string strSql = sbSql.ToString();
            sbSql = null;

            return strSql;
        }

        public virtual bool IsExistDB(string DataBaseName)
        {
            return false;
        }
    }
}
