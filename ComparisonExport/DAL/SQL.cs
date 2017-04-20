using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace DAL
{
    class SQL : DBHelper
    {
        //private static string ConntionString = ConfigurationSettings.AppSettings["SqlConnectionString"];
        //private static string ConnectionString = string.Empty;
        //private string ConnectionString = string.Empty;
        private char mEscape = (char)2;
        public SQL()
        {
            ConnectionString = System.Configuration.ConfigurationManager.AppSettings["SqlConnectionString"];
        }

        public SQL(string ConnectionStr)
        {
            ConnectionString = System.Configuration.ConfigurationManager.AppSettings[ConnectionStr];
        }

        public SQL(string ConnectionStr, bool IsAppSettings)
        {
            if (IsAppSettings)
            {
                ConnectionString = System.Configuration.ConfigurationManager.AppSettings[ConnectionStr];
            }
            else
            {
                ConnectionString = ConnectionStr;
            }
        }

        public SQL(string DataBase, string Address, string Port, string UserName, string PassWord)
        {
            ConnectionString = string.Format("Data Source={0}{4};Initial Catalog={1};User Id={2};Password={3}", Address, string.IsNullOrEmpty(DataBase) ? "master" : DataBase, UserName, PassWord, string.IsNullOrEmpty(Port) ? "" : "," + Port);
        }

        private  SqlConnection CreateNewConn()
        {
            return new SqlConnection(ConnectionString);
        }

        public override bool IsConnected()
        {
            bool bolRet = false;
            SqlConnection sqlConn = CreateNewConn();
            try
            {
                sqlConn.Open();
                if (sqlConn.State == ConnectionState.Open)
                {
                    bolRet = true;
                }
            }
            catch (SqlException ex)
            {               
            }
            finally
            {
                sqlConn.Close();
            }

            return bolRet;
        }
        
        /// <summary>
        /// Add by liupei 2010-3-31
        /// 根据简单sql返回结果集
        /// </summary>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public override DataTable GetDataTable(string Sql)
        {
            SqlConnection sqlConn = CreateNewConn();
            SqlCommand sqlComm = new SqlCommand(Sql, sqlConn);
            SqlDataAdapter sqlAdapter = new SqlDataAdapter(sqlComm);
            DataTable dtRet = new DataTable();

            try
            {
                sqlConn.Open();
                sqlAdapter.Fill(dtRet);

                if (dtRet.Rows.Count == 0)
                {
                    dtRet.Dispose();
                    dtRet = null;
                }
            }
            catch (SqlException ex)
            {
                throw ex;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
                sqlComm.Dispose();
                sqlAdapter.Dispose();
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

        public override DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len, string PreHitWord, string PostHitWord)
        {
            return GetDataTable(Fields, TableName, Condition, Order, Key, Start, Len);
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
            SqlConnection sqlConn = CreateNewConn();
            SqlCommand sqlComm = new SqlCommand(Sql, sqlConn);
            
            int iRet = -1;
            try
            {
               sqlConn.Open();
               iRet = sqlComm.ExecuteNonQuery();

            }
            catch (SqlException ex)
            {
                return iRet;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
                sqlComm.Dispose();
            }

            return iRet;
        }

        /// <summary>
        /// add by liupei 2010-4-1
        /// 使用事务执行sql
        /// </summary>
        /// <param name="SqlList"></param>
        /// <returns></returns>
        public override bool ExecuteSql(List<string> SqlList)
        {
            SqlConnection sqlConn = CreateNewConn();
            SqlCommand sqlComm = new SqlCommand();
            SqlTransaction sqlTrans = null;
            try
            {
                sqlConn.Open();
                sqlTrans = sqlConn.BeginTransaction();
                sqlComm.Connection = sqlConn;
                sqlComm.Transaction = sqlTrans;
                foreach (string strSql in SqlList)
                {
                    sqlComm.CommandText = strSql;
                    sqlComm.ExecuteNonQuery();
                }

                sqlTrans.Commit();
            }
            catch (SqlException ex)
            {
                sqlTrans.Rollback();
                //throw ex;
                return false;
            }
            finally
            {
                sqlTrans.Dispose(); 
                sqlConn.Close();
                sqlConn.Dispose();
                sqlComm.Dispose();

            }
            return true;
        }

        public override DataSet RunProcedure(string ProcName)
        {
            SqlConnection sqlConn = CreateNewConn();
            SqlCommand sqlComm = new SqlCommand(ProcName, sqlConn);
            sqlComm.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter sqlAdapter = new SqlDataAdapter(sqlComm);
            DataSet dsRet = new DataSet();

            try
            {
                sqlConn.Open();
                sqlAdapter.Fill(dsRet);

                if (dsRet.Tables.Count == 0)
                {
                    dsRet.Dispose();
                    dsRet = null;
                }
            }
            catch (SqlException ex)
            {
                throw ex;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
                sqlComm.Dispose();
                sqlAdapter.Dispose();
            }
            return dsRet;
        }

        public override DataSet RunProcedure(string ProcName, IDataParameter[] Parameters)
        {
            SqlConnection sqlConn = CreateNewConn();
            SqlCommand sqlComm = new SqlCommand(ProcName,sqlConn);
            sqlComm.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter sqlAdapter = new SqlDataAdapter(sqlComm);
            DataSet dsRet = new DataSet();

            foreach (SqlParameter parameter in Parameters)
            {
                if (parameter != null)
                {
                    // 检查未分配值的输出参数,将其分配以DBNull.Value.
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                    sqlComm.Parameters.Add(parameter);
                }
            }

            try
            {
                sqlConn.Open();
                sqlAdapter.Fill(dsRet);

                if (dsRet.Tables.Count == 0)
                {
                    dsRet.Dispose();
                    dsRet = null;
                }
            }
            catch (SqlException ex)
            {
                throw ex;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
                sqlComm.Dispose();
                sqlAdapter.Dispose();
            }
            return dsRet;
        }

        public override DataSet RunProcedure(string ProcName, Dictionary<string, object> Parameters)
        {
            SqlParameter[] MySqlParam = new SqlParameter[Parameters.Count];

            Dictionary<string, object>.Enumerator enParam = Parameters.GetEnumerator();

            int iIndex = 0;
            while (enParam.MoveNext())
            {
                MySqlParam[iIndex++] = new SqlParameter(enParam.Current.Key, enParam.Current.Value);
            }

            return RunProcedure(ProcName, MySqlParam);
        }

        public override bool MoveData(string SrcTable, string DestTable, string Condition)
        {
            return false;
        }

        public override bool MoveData(string SrcTable, string DestTable, string Condition, string[] Fields, string[] Values)
        {
            return false;
        }

        public override int ExecuteSql(string Sql, IDataParameter[] Param)
        {
            SqlConnection sqlConn = CreateNewConn();
            SqlCommand sqlComm = new SqlCommand(Sql, sqlConn);
            foreach (SqlParameter sqlParam in Param)
            {
                sqlComm.Parameters.Add(sqlParam);
            }

            int iRet = 0;
            try
            {
                sqlConn.Open();
                iRet = sqlComm.ExecuteNonQuery();

            }
            catch (SqlException ex)
            {
                return iRet;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
                sqlComm.Dispose();
            }

            return iRet;
        }
        public override int ExecuteSql(string Sql, Dictionary<string, object> Parameters)
        {
            SqlParameter[] MySqlParam = new SqlParameter[Parameters.Count];

            Dictionary<string, object>.Enumerator enParam = Parameters.GetEnumerator();

            int iIndex = 0;
            while (enParam.MoveNext())
            {
                MySqlParam[iIndex++] = new SqlParameter(enParam.Current.Key, enParam.Current.Value);
            }

            return ExecuteSql(Sql, MySqlParam);
        }

        public override bool Insert(string TableName, string[] Fields, object[] Values)
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

        public override bool Update(string TableName, string Condition, string[] Fields, object[] Values)
        {
            StringBuilder sbSql = new StringBuilder();
            for (int i = 0; i < Fields.Length && i < Values.Length; i++)
            {
                sbSql.AppendFormat("{0}={1},", Fields[i], Values[i] == null ? "NULL" : string.Format("'{0}'", CharacterHandler(Values[i].ToString())));
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

        private string CharacterHandler(string CharacterStr)
        {
            string strRet = CharacterStr.Replace("'", "'+char(39)+'");
            return strRet;
        }

        public override bool AppendDataText(string TableName, string DataText)
        {
            throw new NotImplementedException();
        }

        public override string RemoveSpecialCharacter(string Condition)
        {
            return string.Format("'{0}'", Condition.Replace("'", ""));
        }

        public override string FilterSpecialCharacter(string Condition)
        {
            //return string.Format("'{0}' escape'{1}' ", Condition.Replace("'", "''").Replace("%", string.Format("{0}%", mEscape))
            //    .Replace("?", string.Format("{0}?", mEscape))
            //    .Replace("[", string.Format("{0}[", mEscape))
            //    .Replace("]", string.Format("{0}]", mEscape))
            //    .Replace("_", string.Format("{0}_", mEscape)), mEscape);
            return string.Format("'{0}'", Condition.Replace("'", "''"));
        }

        public override string FilterSpecialCharacter(string Condition, string Prefix, string Postfix)
        {
            return string.Format("'{2}{0}{3}' escape'{1}' ", Condition.Replace("'", "''").Replace("%", string.Format("{0}%", mEscape))
                .Replace("?", string.Format("{0}?", mEscape))
                .Replace("[", string.Format("{0}[", mEscape))
                .Replace("]", string.Format("{0}]", mEscape))
                .Replace("_", string.Format("{0}_", mEscape)), mEscape, Prefix, Postfix);
        }

        public override string GetSql(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len)
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

            return strSql;
        }

        public override bool IsExistDB(string DataBaseName)
        {
            int iCount = GetCount("master.dbo.sysdatabases", string.Format("NAME='{0}'", DataBaseName));
            if (iCount > 0)
            {
                return true;
            }
            return false;
        }
    }
}
