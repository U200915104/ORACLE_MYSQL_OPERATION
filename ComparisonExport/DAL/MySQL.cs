using System.Configuration;
using System.Data;
using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Text;

namespace DAL
{
    class MySQL:DBHelper
    {

        //Database=dbname;Data Source=127.0.0.1;User Id=root;Password=root;pooling=false;CharSet=utf8;port=3306
        //private string ConnectionString = string.Empty;
        
        public MySQL()
        {
            ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["MySqlConnection"].ConnectionString;
        }

        public MySQL(string ConnectionStr)
        {
            ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings[ConnectionStr].ConnectionString;
        }

        public MySQL(string ConnectionStr, bool IsAppSettings)
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

        public MySQL(string DataBase, string Address, string Port, string UserName, string PassWord)
        {
            ConnectionString = string.Format("Data Source={0};Database={1};User Id={2};Password={3};pooling=true;CharSet=utf8;port={4};", Address, string.IsNullOrEmpty(DataBase) ? "mysql" : DataBase, UserName, PassWord, Port);
        }

        private  MySqlConnection CreateNewConn()
        {
            bool bolFlg = false;
            MySqlConnection mySqlConn = new MySqlConnection(ConnectionString);
            try
            {
                mySqlConn.Open();
                if (mySqlConn.State == ConnectionState.Open)
                {
                    bolFlg = true;
                }
            }
            catch (MySqlException ex)
            {
                bolFlg = false;
            }
            finally
            {
                mySqlConn.Close();
            }
            if (bolFlg)
            {
                return mySqlConn;
            }

            string strMasterAddr = GetDataBaseAddr(System.Configuration.ConfigurationManager.ConnectionStrings["MySqlConnection"].ConnectionString);
            string strSlaveAddr = GetDataBaseAddr(System.Configuration.ConfigurationManager.ConnectionStrings["MySqlConnectionSlave"].ConnectionString);
            string strCurAddr = GetDataBaseAddr(ConnectionString);
            if (strCurAddr == strMasterAddr || strCurAddr == strSlaveAddr)
            {
                ConnectionString = ConnectionString.Replace(strCurAddr, strCurAddr == strMasterAddr ? strSlaveAddr : strMasterAddr);
            }
            IsSlaveServer = strCurAddr == strMasterAddr ? true : false;
            return new MySqlConnection(ConnectionString);


        }

        public override bool IsConnected()
        {
            bool bolRet = false;
            MySqlConnection mySqlConn = new MySqlConnection(ConnectionString);
            try
            {
                mySqlConn.Open();
                if (mySqlConn.State == ConnectionState.Open)
                {
                    bolRet = true;
                }
            }
            catch (MySqlException ex)
            {
            }
            finally
            {
                mySqlConn.Close();
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
            MySqlConnection sqlConn = CreateNewConn();
            MySqlCommand sqlComm = new MySqlCommand(Sql, sqlConn);
            MySqlDataAdapter sqlAdapter = new MySqlDataAdapter(sqlComm);
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
            catch (MySqlException ex)
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

        public override DataTable GetDataTable(string Fields, string TableName, string Condition, string Order, string Key, int Start, int Len)
        {
            StringBuilder sbSql = new StringBuilder();

            sbSql.AppendFormat("SELECT {0} FROM {1} ", Key, TableName);
            if (!string.IsNullOrEmpty(Condition))
            {
                sbSql.AppendFormat("WHERE {0} ", Condition);
            }
            if (!string.IsNullOrEmpty(Order))
            {
                sbSql.AppendFormat("ORDER BY {0} ", Order);
            }

            sbSql.AppendFormat("LIMIT {0},{1}", Start - 1, Len);

            DataTable dtKeys = GetDataTable(sbSql.ToString());
            if (dtKeys == null)
            {
                return null;
            }

            sbSql.Clear();
            StringBuilder sbKey = new StringBuilder();
            foreach (DataRow drKey in dtKeys.Rows)
            {
                if (sbKey.Length > 0)
                {
                    sbKey.Append(",");
                }
                sbKey.Append(drKey[Key]);
            }

            sbSql.AppendFormat("SELECT {0} FROM {1} where {2} in ({3}) ", Fields, TableName,Key,sbKey.ToString());
            if (!string.IsNullOrEmpty(Order))
            {
                sbSql.AppendFormat("ORDER BY {0} ", Order);
            }

            string strSql = sbSql.ToString();
            sbSql = null;
            sbKey = null;
            return GetDataTable(strSql);
        }

        public override int ExecuteSql(string Sql)
        {
            MySqlConnection sqlConn = CreateNewConn();
            MySqlCommand sqlComm = new MySqlCommand(Sql, sqlConn);
            

            int iRet = -1;
            try
            {
                sqlConn.Open();
                iRet = sqlComm.ExecuteNonQuery();

            }
            catch (MySqlException ex)
            {
                throw ex;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
                sqlComm.Dispose();
            }

            return iRet;
        }

        public override int ExecuteSql(string Sql, IDataParameter[] Param)
        {
            MySqlConnection MySqlConn = CreateNewConn();
            MySqlCommand MySqlComm = new MySqlCommand(Sql, MySqlConn);
            foreach (MySqlParameter MySqlParam in Param)
            {
                MySqlComm.Parameters.Add(MySqlParam);
            }

            int iRet = -1;
            try
            {
                MySqlConn.Open();
                iRet = MySqlComm.ExecuteNonQuery();

            }
            catch (MySqlException ex)
            {
                throw ex;
                //return iRet;
            }
            finally
            {
                MySqlConn.Close();
                MySqlConn.Dispose();
                MySqlComm.Dispose();
            }

            return iRet;
        }

        public override int ExecuteSql(string Sql, Dictionary<string, object> Parameters)
        {
            MySqlParameter[] MySqlParam = new MySqlParameter[Parameters.Count];

            Dictionary<string, object>.Enumerator enParam = Parameters.GetEnumerator();

            int iIndex = 0;
            while (enParam.MoveNext())
            {
                MySqlParam[iIndex++] = new MySqlParameter(enParam.Current.Key, enParam.Current.Value);
            }

            return ExecuteSql(Sql, MySqlParam);
        }

        public override bool ExecuteSql(List<string> SqlList)
        {
            MySqlConnection MySqlConn = CreateNewConn();
            MySqlCommand MySqlComm = new MySqlCommand();
            MySqlTransaction MysqlTrans = null;
            try
            {
                MySqlConn.Open();
                MysqlTrans = MySqlConn.BeginTransaction();
                MySqlComm.Connection = MySqlConn;
                MySqlComm.Transaction = MysqlTrans;
                foreach (string strSql in SqlList)
                {
                    MySqlComm.CommandText = strSql;
                    MySqlComm.ExecuteNonQuery();
                }

                MysqlTrans.Commit();
            }
            catch (MySqlException ex)
            {
                MysqlTrans.Rollback();
                throw ex;
                //return false;
            }
            finally
            {
                MysqlTrans.Dispose();
                MySqlConn.Close();
                MySqlConn.Dispose();
                MySqlComm.Dispose();

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
            MySqlParameter[] MySqlParam = new MySqlParameter[iLen];
            for (int i = 0; i < iLen; i++)
            {                
                sbFields.AppendFormat("{0},", Fields[i]);
                sbValues.AppendFormat("@{0},", Fields[i]);
                MySqlParam[i] = new MySqlParameter(Fields[i], Values[i]);
            }

            string strSql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", TableName, sbFields.ToString().Trim(','), sbValues.ToString().Trim(','));

            sbFields = null;
            sbValues = null;

            int iRet= ExecuteSql(strSql, MySqlParam);
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
            MySqlParameter[] MySqlParam = new MySqlParameter[iLen];
            for (int i = 0; i < iLen; i++)
            {
                if (Values[i] == null)
                {
                    sbSql.AppendFormat("{0}=NULL,", Fields[i]);
                    continue;
                }
                sbSql.AppendFormat("{0}=@{0},", Fields[i]);
                MySqlParam[i] = new MySqlParameter(Fields[i], Values[i]);
            }

            string strSql = string.Format("UPDATE {0} SET {1}", TableName, sbSql.ToString().Trim(','));
            if (!string.IsNullOrEmpty(Condition))
            {
                strSql = string.Format("{0} WHERE {1}", strSql, Condition);
            }

            sbSql = null;
            int iRet = -1;
            try
            {
                iRet = ExecuteSql(strSql, MySqlParam);
                if (ExecuteSql(strSql, MySqlParam) > -1)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (System.Exception)
            {
                throw;
            }
          
        }

        public override DataSet RunProcedure(string ProcName)
        {
            MySqlConnection MySqlConn = CreateNewConn();
            MySqlCommand MySqlComm = new MySqlCommand(ProcName, MySqlConn);
            MySqlComm.CommandType = CommandType.StoredProcedure;

            MySqlDataAdapter MySqlAdapter = new MySqlDataAdapter(MySqlComm);
            DataSet dsRet = new DataSet();            

            try
            {
                MySqlConn.Open();
                MySqlAdapter.Fill(dsRet);

                if (dsRet.Tables.Count == 0)
                {
                    dsRet.Dispose();
                    dsRet = null;
                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
            finally
            {
                MySqlConn.Close();
                MySqlConn.Dispose();
                MySqlComm.Dispose();
                MySqlAdapter.Dispose();
            }
            return dsRet;
        }

        public override DataSet RunProcedure(string ProcName, IDataParameter[] Parameters)
        {
            MySqlConnection MySqlConn = CreateNewConn();
            MySqlCommand MySqlComm = new MySqlCommand(ProcName, MySqlConn);
            MySqlComm.CommandType = CommandType.StoredProcedure;

            MySqlDataAdapter MySqlAdapter = new MySqlDataAdapter(MySqlComm);
            DataSet dsRet = new DataSet();

            foreach (MySqlParameter parameter in Parameters)
            {
                if (parameter != null)
                {
                    // 检查未分配值的输出参数,将其分配以DBNull.Value.
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = null;
                    }
                    MySqlComm.Parameters.Add(parameter);
                }
            }

            try
            {
                MySqlConn.Open();
                MySqlAdapter.Fill(dsRet);

                if (dsRet.Tables.Count == 0)
                {
                    dsRet.Dispose();
                    dsRet = null;
                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
            finally
            {
                MySqlConn.Close();
                MySqlConn.Dispose();
                MySqlComm.Dispose();
                MySqlAdapter.Dispose();
            }
            return dsRet;
        }

        public override DataSet RunProcedure(string ProcName, Dictionary<string, object> Parameters)
        {
            MySqlParameter[] MySqlParam = new MySqlParameter[Parameters.Count];

            Dictionary<string, object>.Enumerator enParam = Parameters.GetEnumerator();

            int iIndex = 0;
            while (enParam.MoveNext())
            {
                MySqlParam[iIndex++] = new MySqlParameter(enParam.Current.Key, enParam.Current.Value);
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

        public override bool IsExistDB(string DataBaseName)
        {
            int iCount = GetCount("mysql.db", string.Format("DB='{0}'", DataBaseName));
            if (iCount > 0)
            {
                return true;
            }
            return false;
        }

        private string GetDataBaseAddr(string ConnectionStr)
        {
            string strAddress = string.Empty;
            string strConnection = ConnectionStr;
            string[] arrParam = strConnection.Split(';');
            foreach (string strParam in arrParam)
            {
                if (strParam.ToLower().IndexOf("data source") > -1)
                {
                    strAddress = strParam.Split('=')[1].Trim();
                    break; 
                }
            }
            return strAddress;
        }
    }
}
