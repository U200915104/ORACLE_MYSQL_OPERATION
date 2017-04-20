using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.OleDb;
using System.Data;


namespace DAL
{
    class Oracle : DBHelper
    {
        public Oracle()
        {
            ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["OracleConnection"].ConnectionString;
        }

        public Oracle(string ConnectionStr)
        {
            ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings[ConnectionStr].ConnectionString;
        }

        public Oracle(string ConnectionStr, bool IsAppSettings)
        {
            if (IsAppSettings)
            {
                ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings[ConnectionStr].ConnectionString;
            }
            else
            {
                ConnectionString = ConnectionStr;
            }
        }
        private OleDbConnection CreateNewConn()
        {
            bool bolFlg = false;
            OleDbConnection oraConn = new OleDbConnection(ConnectionString);
            try
            {
                
                oraConn.Open();
                
                if (oraConn.State == ConnectionState.Open)
                {
                    bolFlg = true;
                }
            }
            catch (Exception ex)
            {
                bolFlg = false;
            }
            finally
            {
                oraConn.Close();
            }
            if (bolFlg)
            {
                return oraConn;
            }

            return null;
            //string strMasterAddr = GetDataBaseAddr(System.Configuration.ConfigurationManager.ConnectionStrings["MySqlConnection"].ConnectionString);
          //  string strSlaveAddr = GetDataBaseAddr(System.Configuration.ConfigurationManager.ConnectionStrings["MySqlConnectionSlave"].ConnectionString);
          //  string strCurAddr = GetDataBaseAddr(ConnectionString);
          //  if (strCurAddr == strMasterAddr || strCurAddr == strSlaveAddr)
          //  {
         //       ConnectionString = ConnectionString.Replace(strCurAddr, strCurAddr == strMasterAddr ? strSlaveAddr : strMasterAddr);
         //   }
         //   IsSlaveServer = strCurAddr == strMasterAddr ? true : false;
         //   return new MySqlConnection(ConnectionString);


        }

        public override DataTable GetDataTable(string Sql)
        {
            OleDbConnection oraConn = CreateNewConn();
            OleDbCommand oraComm = new OleDbCommand(Sql, oraConn);
            OleDbDataAdapter oraAdapter = new OleDbDataAdapter(oraComm);
            DataTable dtRet = new DataTable();

            try
            {
                oraConn.Open();
                oraAdapter.Fill(dtRet);

                if (dtRet.Rows.Count == 0)
                {
                    dtRet.Dispose();
                    dtRet = null;
                }
            }
            catch (OleDbException ex)
            {
                throw ex;
            }
            finally
            {
                oraConn.Close();
                oraConn.Dispose();
                oraComm.Dispose();
                oraAdapter.Dispose();
            }

            return dtRet;
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


            string strSql = string.Format("SELECT {0} FROM ( SELECT TBA.*, ROWNUM RN FROM ({1}) TBA WHERE ROWNUM < {2} ) WHERE RN >= {3}",Fields, sbSql.ToString(), Start + Len, Start);


            sbSql = null;

            return GetDataTable(strSql);
        }

        public override int ExecuteSql(string Sql)
        {
            OleDbConnection oraConn = CreateNewConn();
            OleDbCommand oraComm = new OleDbCommand(Sql, oraConn);


            int iRet = -1;
            try
            {
                oraConn.Open();
                iRet = oraComm.ExecuteNonQuery();

            }
            catch (OleDbException ex)
            {
                return iRet;
            }
            finally
            {
                oraConn.Close();
                oraConn.Dispose();
                oraComm.Dispose();
            }

            return iRet;
        }

        public override int ExecuteSql(string Sql, IDataParameter[] Param)
        {
            OleDbConnection oraConn = CreateNewConn();
            OleDbCommand oraComm = new OleDbCommand(Sql, oraConn);
            foreach (OleDbParameter oraParam in Param)
            {
                oraComm.Parameters.Add(oraParam);
            }

            int iRet = -1;
            try
            {
                oraConn.Open();
                iRet = oraComm.ExecuteNonQuery();

            }
            catch (OleDbException ex)
            {
                throw ex;
                //return iRet;
            }
            finally
            {
                oraConn.Close();
                oraConn.Dispose();
                oraComm.Dispose();
            }

            return iRet;
        }

        public override int ExecuteSql(string Sql, Dictionary<string, object> Parameters)
        {
            OleDbParameter[] oraParam = new OleDbParameter[Parameters.Count];

            Dictionary<string, object>.Enumerator enParam = Parameters.GetEnumerator();

            int iIndex = 0;
            while (enParam.MoveNext())
            {
                oraParam[iIndex++] = new OleDbParameter(enParam.Current.Key, enParam.Current.Value);
            }

            return ExecuteSql(Sql, oraParam);
        }

        public override bool ExecuteSql(List<string> SqlList)
        {
            OleDbConnection oraConn = CreateNewConn();
            OleDbCommand oraComm = new OleDbCommand();
            OleDbTransaction oraTrans = null;
            try
            {
                oraConn.Open();
                oraTrans = oraConn.BeginTransaction();
                oraComm.Connection = oraConn;
                oraComm.Transaction = oraTrans;
                foreach (string strSql in SqlList)
                {
                    oraComm.CommandText = strSql;
                    oraComm.ExecuteNonQuery();
                }

                oraTrans.Commit();
            }
            catch (OleDbException ex)
            {
                oraTrans.Rollback();
                throw ex;
                //return false;
            }
            finally
            {
                oraTrans.Dispose();
                oraConn.Close();
                oraConn.Dispose();
                oraComm.Dispose();

            }
            return true;
        }

        public override bool Insert(string TableName, string[] Fields, object[] Values)
        {
            if (Fields.Length != Values.Length)
            {
                return false;
            }

            int iLen = Fields.Length;
            StringBuilder sbFields = new StringBuilder();
            StringBuilder sbValues = new StringBuilder();
            OleDbParameter[] oraParam = new OleDbParameter[iLen];
            for (int i = 0; i < iLen; i++)
            {
                sbFields.AppendFormat("{0},", Fields[i]);
                sbValues.AppendFormat(":{0},", Fields[i]);
                oraParam[i] = new OleDbParameter(Fields[i], Values[i]);
            }

            string strSql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", TableName, sbFields.ToString().Trim(','), sbValues.ToString().Trim(','));

            sbFields = null;
            sbValues = null;

            int iRet = ExecuteSql(strSql, oraParam);
            if (iRet > 0)
            {
                return true;
            }
            return false;

        }

        public override bool Update(string TableName, string Condition, string[] Fields, object[] Values)
        {
            if (Fields.Length != Values.Length)
            {
                return false;
            }

            int iLen = Fields.Length;
            StringBuilder sbSql = new StringBuilder();
            OleDbParameter[] oraParam = new OleDbParameter[iLen];
            for (int i = 0; i < iLen; i++)
            {
                if (Values[i] == null)
                {
                    sbSql.AppendFormat("{0}=NULL,", Fields[i]);
                    continue;
                }
                sbSql.AppendFormat("{0}=:{0},", Fields[i]);
                oraParam[i] = new OleDbParameter(Fields[i], Values[i]);
            }

            string strSql = string.Format("UPDATE {0} SET {1}", TableName, sbSql.ToString().Trim(','));
            if (!string.IsNullOrEmpty(Condition))
            {
                strSql = string.Format("{0} WHERE {1}", strSql, Condition);
            }

            sbSql = null;

            if (ExecuteSql(strSql, oraParam) > -1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override DataSet RunProcedure(string ProcName)
        {
            OleDbConnection oraConn = CreateNewConn();
            OleDbCommand oraComm = new OleDbCommand(ProcName, oraConn);
            oraComm.CommandType = CommandType.StoredProcedure;

            OleDbDataAdapter oraAdapter = new OleDbDataAdapter(oraComm);
            DataSet dsRet = new DataSet();

            try
            {
                oraConn.Open();
                oraAdapter.Fill(dsRet);

                if (dsRet.Tables.Count == 0)
                {
                    dsRet.Dispose();
                    dsRet = null;
                }
            }
            catch (OleDbException ex)
            {
                throw ex;
            }
            finally
            {
                oraConn.Close();
                oraConn.Dispose();
                oraComm.Dispose();
                oraAdapter.Dispose();
            }
            return dsRet;
        }

        public override DataSet RunProcedure(string ProcName, IDataParameter[] Parameters)
        {
            OleDbConnection oraConn = CreateNewConn();
            OleDbCommand oraComm = new OleDbCommand(ProcName, oraConn);
            oraComm.CommandType = CommandType.StoredProcedure;

            OleDbDataAdapter oraAdapter = new OleDbDataAdapter(oraComm);
            DataSet dsRet = new DataSet();

            foreach (OleDbParameter parameter in Parameters)
            {
                if (parameter != null)
                {
                    // 检查未分配值的输出参数,将其分配以DBNull.Value.
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = null;
                    }
                    oraComm.Parameters.Add(parameter);
                }
            }

            try
            {
                oraConn.Open();
                oraAdapter.Fill(dsRet);

                if (dsRet.Tables.Count == 0)
                {
                    dsRet.Dispose();
                    dsRet = null;
                }
            }
            catch (OleDbException ex)
            {
                throw ex;
            }
            finally
            {
                oraConn.Close();
                oraConn.Dispose();
                oraComm.Dispose();
                oraAdapter.Dispose();
            }
            return dsRet;
        }

        public override DataSet RunProcedure(string ProcName, Dictionary<string, object> Parameters)
        {
            OleDbParameter[] MySqlParam = new OleDbParameter[Parameters.Count];

            Dictionary<string, object>.Enumerator enParam = Parameters.GetEnumerator();

            int iIndex = 0;
            while (enParam.MoveNext())
            {
                MySqlParam[iIndex++] = new OleDbParameter(enParam.Current.Key, enParam.Current.Value);
            }

            return RunProcedure(ProcName, MySqlParam);
        }

        public override string RemoveSpecialCharacter(string Condition)
        {
            return string.Format("'{0}'", Condition.Replace("'", ""));
        }

        public override string FilterSpecialCharacter(string Condition)
        {
            //like 查询时，_为特殊字符
            //return string.Format("'{0}'", Condition.Replace(@"\", @"\\\")
            //    .Replace("%", @"\%").Replace("_", @"\_").Replace("'", @"\'"));        

            Condition = Condition.Replace(@"\", @"\\");
            Condition = Condition.Replace("%", @"\%");
            Condition = Condition.Replace("'", @"\'");

            return string.Format("'{0}'", Condition);
        }

        public override string FilterSpecialCharacter(string Condition, string Prefix, string Postfix)
        {
            Condition = Condition.Replace(@"\", @"\\");
            Condition = Condition.Replace("%", @"\%");
            Condition = Condition.Replace("_", @"\_");
            Condition = Condition.Replace("'", @"\'");

            return string.Format("'{1}{0}{2}'", Condition, Prefix, Postfix);
        }

        public override string GetSql(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len)
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

            return strSql;
        }

        public override bool IsConnected()
        {
            if (CreateNewConn() != null)
            {
                return true;
            }
            return false;
        }
    }
}
