﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using DAL;
using System.Data;
using System.Threading;
using Microsoft.Win32;
using System.IO;

namespace ComparisonExport
{
    /// <summary>
    /// MySqlToOracle.xaml 的交互逻辑
    /// </summary>
    public partial class MySqlToOracle : Window
    {
       private  static object mLock = new object();
       private static object mLockPB = new object();
       private List<Thread> mThread;
       private int mThreadCount;
       private string mServerIP;
       private int mQueryCount=5000;
        public MySqlToOracle()
        {
            InitializeComponent();

            txtAddr.Text = "10.2.64.15";
            txtPort.Text = "3306";
            txtUserName.Text = "root";
            txtPassword.Text = "root";
            txtDBName.Text = "faceserverdb";

            txtAddrTo.Text = "10.2.64.247";
            txtPortTo.Text = "1521";
            txtUserNameTo.Text = "tckts";
            txtPasswordTo.Text = "tckts";
            txtOraService.Text = "rxbd2";
            txtFileName.Text = "d:\\logfile.txt";
        }

        private void btnOK_Click(object sender, RoutedEventArgs e)
        {
            string strMySqlConn = string.Format("Data Source={0};Database={1};User Id={2};Password={3};pooling=true;CharSet=utf8;port={4};", txtAddr.Text.Trim(),
                txtDBName.Text.Trim(), txtUserName.Text.Trim(), txtPassword.Text.Trim(), txtPort.Text.Trim());
            string strOraConn = string.Format("Provider=OraOLEDB.Oracle.1;User ID={0};Password={1};Data Source=(DESCRIPTION = (ADDRESS_LIST= (ADDRESS = (PROTOCOL = TCP)(HOST = {2})(PORT = {3}))) (CONNECT_DATA = (SERVICE_NAME = {4})))", txtUserNameTo.Text.Trim(),
                txtPasswordTo.Text.Trim(), txtAddrTo.Text.Trim(), txtPortTo.Text.Trim(), txtOraService.Text.Trim());

            mServerIP = txtAddr.Text.Trim();
            mThreadCount=Convert.ToInt32(txtThreadCount.Text.Trim());
            mThread = new List<Thread>(mThreadCount);
            mQueryCount = Convert.ToInt32(txtQueryCount.Text.Trim());
           // return;
            Thread thExport = new Thread(new ParameterizedThreadStart(Export));
            thExport.Start(new string[] { strMySqlConn, strOraConn, txtUserNameTo.Text.Trim(),txtFileName.Text.Trim(),txtThreadCount.Text.Trim()});


           // bolRet = dbHelper.Insert("VIEW_PC_MB", new string[] { "MBID" }, new object[] { 3 });
        }

        private void Export(object arg)
        {
            int iLen = 5000;
            string[] arrArg = (string[])arg;
            string strMySqlConn = arrArg[0];
            string strOraConn = arrArg[1];
            string strOraUserName = arrArg[2];
            string strLogFile = arrArg[3];
            int iThreadCount=Convert.ToInt32(arrArg[4]);

            File.Delete(strLogFile);
            
            DBHelper dbHelperMySql = DBHelper.GetInstance("MySql", strMySqlConn, false);
            DBHelper dbHelperOra = DBHelper.GetInstance("Oracle", strOraConn, false);

            //if(dbHelperOra.GetCount("dba_tables",string.Format("owner = '{0}' and table_name = 'VIEW_PC_MB'",strOraUserName.ToUpper()))==0)
            //{
            //    string strSql = string.Format("CREATE TABLE {0}.VIEW_PC_MB (\"MBID\" CHAR(32 BYTE), \"MBA\" VARCHAR2(20 BYTE),\"MBB\" BLOB)", strOraUserName.ToUpper());
            //   // string strSql = "CREATE TABLE 'VIEW_PC_MB' ('MBID' NUMBER NOT NULL ENABLE, 'MBA' VARCHAR2(20 BYTE),'MBB' BLOB)";
            //    if (dbHelperOra.ExecuteSql(strSql)<0)
            //    {
            //        MessageBox.Show("创建VIEW_PC_MB失败！");
            //        return;
            //    }
            //}

            DataTable dtNames = dbHelperMySql.GetDataTable("name", "tableinfo",string.Format("serverip='{0}'",mServerIP));
            long lCount = 0;
            foreach (DataRow drName in dtNames.Rows)
            {
                lCount =lCount+ dbHelperMySql.GetCount(drName["name"].ToString(), "flag=0");
            }

            if (lCount == 0)
            {
                MessageBox.Show("导出完成！");
                return;
            }

            Dispatcher.Invoke(new Action(delegate { pbExport.Maximum = lCount; tbProcess.Text = string.Format("0/{0}", lCount); }));
            DataTable dtInfos;
            foreach (DataRow drName in dtNames.Rows)
            {
                ExportTable(drName["NAME"].ToString(), iThreadCount, strMySqlConn, strOraConn, strLogFile, lCount);
                //int iCurIndex = 1;
                //while (true)
                //{
                //    dtInfos = dbHelperMySql.GetDataTable("ID,ZJHM,ZPTZ", drName["name"].ToString(), "", "ID", "ID", iCurIndex, iLen);
                //    if (dtInfos == null)
                //    {
                //        break;
                //    }
                //    iCurIndex = iCurIndex + iLen;

                //    foreach (DataRow drInfo in dtInfos.Rows)
                //    {          
                //        if (!dbHelperOra.Insert("VIEW_PC_MB", new string[] { "MBID", "MBB" }, new object[] { drInfo["ZJHM"], drInfo["ZPTZ"] }))
                //        { 
                //            File.AppendAllText(strLogFile,string.Format("{0}-{1}-{2}\r\n",drInfo["ID"],drInfo["ZJHM"],drName["NAME"]));
                //            Dispatcher.Invoke(new Action(delegate { txtErrorCount.Text = (Convert.ToInt32(txtErrorCount.Text) + 1).ToString(); }));
                //        }
                //        Dispatcher.Invoke(new Action(delegate { pbExport.Value = pbExport.Value + 1; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value,lCount); }));
                //    }
                //}
            }

            //MessageBox.Show("导出完成！");
        }

        private void btnBrower_Click(object sender, RoutedEventArgs e)
        {
            SaveFileDialog sfd = new SaveFileDialog();
            sfd.Filter = "txt文件(*.txt)|*.txt";

            if (sfd.ShowDialog() != true)
            {
                return;
            }

            txtFileName.Text = sfd.FileName;
        }

        private void ExportTable(string Table,int ThreadCount,string MySqlConn,string OraConn,string LogFile,long Count)
        {
            int iLen = mQueryCount;
            DBHelper dbHelper = DBHelper.GetInstance("MySql", MySqlConn, false);
            //int iCount = dbHelper.GetCount(Table, "");
            int iCurIndex = 1;
            while (true)
            {
                DataTable dtInfos = dbHelper.GetDataTable("ID,ZJHM,ZPTZ,flag", Table, "", "ID", "ID", iCurIndex, iLen);
                if (dtInfos == null)
                {
                    break;
                }
                iCurIndex = iCurIndex + dtInfos.Rows.Count;

                Thread thExportTable = null;
                if (mThread.Count < mThreadCount)
                {
                    Thread thExport = new Thread(new ParameterizedThreadStart(ExportThread));
                    mThread.Add(thExport);
                    thExportTable = thExport;
                }
                else
                {

                    while (true)
                    {
                        thExportTable = GetThread();
                        if (thExportTable != null)
                        {
                            break;
                        }

                        Thread.Sleep(5000);
                    }
                }
                //ThreadPool.QueueUserWorkItem(new WaitCallback(ExportThread), new object[] { Table, MySqlConn, OraConn, LogFile, dtInfos, Count });
                thExportTable.Start(new object[] { Table, MySqlConn, OraConn, LogFile, dtInfos, Count });

            }
        }

        private Thread GetThread()
        {
            Thread thCur=null;
            foreach (Thread thd in mThread)
            {
                if (thd.ThreadState == ThreadState.Stopped||thd.ThreadState==ThreadState.Unstarted)
                {
                    thCur = thd;
                    break;
                }
            }
            if (thCur != null)
            {
                mThread.Remove(thCur);
                Thread thExport = new Thread(new ParameterizedThreadStart(ExportThread));
                mThread.Add(thExport);
                return thExport;
            }

            return null;
        }

        private void ExportThread(object Arg)
        {

            int iLen = 5000;
            object[] arrArg = (object[])Arg;
            string strTable = (string)arrArg[0];
            string strMySqlConn = (string)arrArg[1];
            string strOraConn = (string)arrArg[2];
            string strFile = (string)arrArg[3];
            DataTable dtInfos = (DataTable)arrArg[4];
            long lCount = (long)arrArg[5];

            DBHelper dbHelperOra = DBHelper.GetInstance("Oracle", strOraConn, false);
            DBHelper dbHelperMySql = DBHelper.GetInstance("MySql", strMySqlConn, false);
    
                foreach (DataRow drInfo in dtInfos.Rows)
                {
                    if ((int)drInfo["flag"] == 1)
                    {
                        continue;
                    }
                    if (!dbHelperOra.Insert("VIEW_PC_MB", new string[] { "MBID", "MBB" }, new object[] { drInfo["ZJHM"], drInfo["ZPTZ"] }))
                    {
                        lock (mLock)
                        {
                            File.AppendAllText(strFile, string.Format("{0}-{1}-{2}\r\n", drInfo["ID"], drInfo["ZJHM"], strTable));
                            Dispatcher.Invoke(new Action(delegate { txtErrorCount.Text = (Convert.ToInt32(txtErrorCount.Text) + 1).ToString(); }));
                        }
                    }

                    dbHelperMySql.Update(strTable, string.Format("id={0}", drInfo["ID"]), new string[] { "flag" }, new object[] { 1 });
                    lock (mLockPB)
                    {
                        Dispatcher.Invoke(new Action(delegate { pbExport.Value = pbExport.Value + 1; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value, lCount);
                        if (pbExport.Value == lCount)
                        {
                            MessageBox.Show("导出完成！");
                            return;
                        }
                        }));

                    }
            }

                dtInfos.Dispose();
                dtInfos = null;


        }
    }
}
