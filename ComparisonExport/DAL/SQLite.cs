using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SQLite;
using System.Text;

namespace DAL
{
    class SQLite : DBHelper
    {
        public SQLite()
        {
            ConnectionString = ConfigurationManager.ConnectionStrings["SQLiteConnectionString"].ConnectionString;
        }

        public SQLite(string ConnectionStr)
        {
            ConnectionString = ConfigurationManager.ConnectionStrings[ConnectionStr].ConnectionString;
        }

        public SQLite(string ConnectionStr, bool IsAppSettings)
        {
            if (IsAppSettings)
            {
                ConnectionString = ConfigurationManager.ConnectionStrings[ConnectionStr].ConnectionString;
            }
            else
            {
                ConnectionString = ConnectionStr;
            }
        }

        private SQLiteConnection CreateNewConn()
        {
            return new SQLiteConnection(ConnectionString);
        }

        public override bool IsConnected()
        {
            bool bolRet = false;
            SQLiteConnection SQLiteConn = CreateNewConn();
            try
            {
                SQLiteConn.Open();
                if (SQLiteConn.State == ConnectionState.Open)
                {
                    bolRet = true;
                }
            }
            catch (SQLiteException ex)
            {
            }
            finally
            {
                SQLiteConn.Close();
            }

            return bolRet;
        }

        public override DataTable GetDataTable(string Sql)
        {
            SQLiteConnection SQLiteConn = CreateNewConn();
            SQLiteCommand SQLiteComm = new SQLiteCommand(Sql, SQLiteConn);
            SQLiteDataAdapter SQLiteAdapter = new SQLiteDataAdapter(SQLiteComm);
            DataTable dtRet = new DataTable();

            try
            {
                SQLiteConn.Open();
                SQLiteAdapter.Fill(dtRet);

                if (dtRet.Rows.Count == 0)
                {
                    dtRet.Dispose();
                    dtRet = null;
                }
            }
            catch (SQLiteException ex)
            {
                throw ex;
            }
            finally
            {
                SQLiteConn.Close();
                SQLiteConn.Dispose();
                SQLiteComm.Dispose();
                SQLiteAdapter.Dispose();
            }

            return dtRet;
        }

        public override DataTable GetDataTable(string Fields, string TableName)
        {
            return GetDataTable(string.Format("SELECT {0} FROM {1} ", Fields, TableName));
        }

        public override DataTable GetDataTable(string Fields, string TableName, string Condition)
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

        public override DataTable GetDataTable(string Fields, string TableName, string Condition, string Order)
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

        public override DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len)
        {
            StringBuilder sbSql = new StringBuilder();

            sbSql.AppendFormat("SELECT {0} FROM {1} ", Fields, TableName);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat("WHERE {0} ", Condition);
            }
            if (!string.IsNullOrEmpty(Order))
            {
                sbSql.AppendFormat("ORDER BY {0} ", Order);
            }

            sbSql.AppendFormat("LIMIT {0},{1}", Start - 1, Len);

            string strSql = sbSql.ToString();
            sbSql = null;

            return GetDataTable(strSql);
        }

        public override DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len, string PreHitWord, string PostHitWord)
        {
            throw new NotImplementedException();
        }

        public override int GetCount(string TableName, string Condition)
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

        public override int ExecuteSql(string Sql)
        {
            SQLiteConnection SQLiteConn = CreateNewConn();
            SQLiteCommand SQLiteComm = new SQLiteCommand(Sql, SQLiteConn);

            int iRet = 0;
            try
            {
                SQLiteConn.Open();
                iRet = SQLiteComm.ExecuteNonQuery();

            }
            catch (SQLiteException ex)
            {
                return iRet;
            }
            finally
            {
                SQLiteConn.Close();
                SQLiteConn.Dispose();
                SQLiteComm.Dispose();
            }
            if (Sql.Substring(0, 6).ToLower() == "delete")
            {
                iRet = 1;
            }
            return iRet;
        }

        public override int ExecuteSql(string Sql, IDataParameter[] Param)
        {
            SQLiteConnection SQLiteConn = CreateNewConn();
            SQLiteCommand SQLiteComm = new SQLiteCommand(Sql, SQLiteConn);

            foreach (SQLiteParameter SQLiteParam in Param)
            {
                SQLiteComm.Parameters.Add(SQLiteParam);
            }

            int iRet = 0;
            try
            {
                SQLiteConn.Open();
                iRet = SQLiteComm.ExecuteNonQuery();

            }
            catch (SQLiteException ex)
            {
                return iRet;
            }
            finally
            {
                SQLiteConn.Close();
                SQLiteConn.Dispose();
                SQLiteComm.Dispose();
            }

            return iRet;
        }

        public override int ExecuteSql(string Sql, Dictionary<string, object> Parameters)
        {
            SQLiteParameter[] MySqlParam = new SQLiteParameter[Parameters.Count];
            Dictionary<string, object>.Enumerator enParam = Parameters.GetEnumerator();

            int iIndex = 0;
            while (enParam.MoveNext())
            {
                MySqlParam[iIndex++] = new SQLiteParameter(enParam.Current.Key, enParam.Current.Value);
            }

            return ExecuteSql(Sql, MySqlParam);
        }
       
    }
}
