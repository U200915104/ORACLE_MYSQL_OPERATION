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
using System.Threading;
using System.IO;
using DAL;
using System.Data;
using Microsoft.Win32;

namespace ComparisonExport
{
    /// <summary>
    /// OracleToMySql.xaml 的交互逻辑
    /// </summary>
    public partial class TwoOracle2Mysql21 : Window
    {
        private static object mLock = new object();
        private static object mLockPB = new object();
        private static Boolean isComplete = true;
        private List<Thread> mThread;
        private int mThreadCount;
        private string mServerIP;
        private int mQueryCount = 2000;
        private bool isPrintSuccess = false;
        public TwoOracle2Mysql21()
        {
            InitializeComponent();

            txtAddr.Text = "10.148.26.169";
            txtPort.Text = "3306";
            txtUserName.Text = "root";
            txtPassword.Text = "root";
            txtDBName.Text = "faceserverdb";

            txtAddrTo.Text = "10.148.63.198";
            txtPortTo.Text = "1521";
            txtUserNameTo.Text = "pcs_yyk_data_jk_rxbd_test";
            txtPasswordTo.Text = "pcs_yyk_data_jk_rxbd_test";
            txtOraService.Text = "czrkzyk";
            txtOraTBName.Text = "V_YB_ASJ_XYRDJ";
            txtBeginIndex.Text = "1";
            txtSingleTableCount.Text = "1000000";
            isPrintSuccess = false;
            txtFileName.Text = "d:\\logfile.txt";

        }

        private void btnOK_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (!isComplete)
                {
                    MessageBox.Show("当前程序正在运行，请勿重复提交请求。");
                    return;
                }
                isComplete = false;
                string strMySqlConn = string.Format("Data Source={0};Database={1};User Id={2};Password={3};pooling=true;CharSet=utf8;port={4};", txtAddr.Text.Trim(),
                txtDBName.Text.Trim(), txtUserName.Text.Trim(), txtPassword.Text.Trim(), txtPort.Text.Trim());
                string strOraConn = string.Format("Provider=OraOLEDB.Oracle.1;User ID={0};Password={1};Data Source=(DESCRIPTION = (ADDRESS_LIST= (ADDRESS = (PROTOCOL = TCP)(HOST = {2})(PORT = {3}))) (CONNECT_DATA = (SERVICE_NAME = {4})))", txtUserNameTo.Text.Trim(),
                    txtPasswordTo.Text.Trim(), txtAddrTo.Text.Trim(), txtPortTo.Text.Trim(), txtOraService.Text.Trim());
                isPrintSuccess = (bool)rb_isPrintSuccess.IsChecked;
                mServerIP = txtAddr.Text.Trim();
                mThreadCount = Convert.ToInt32(txtThreadCount.Text.Trim());
                mThread = new List<Thread>(mThreadCount);
                mQueryCount = Convert.ToInt32(txtQueryCount.Text.Trim());
                // return;
                Thread thExport = new Thread(new ParameterizedThreadStart(Export));
                thExport.Start(new string[] { strMySqlConn, strOraConn, txtUserNameTo.Text.Trim(), txtFileName.Text.Trim(), txtThreadCount.Text.Trim(), txtOraTBName.Text.Trim(), txtBeginIndex.Text.Trim(), txtSingleTableCount.Text.Trim() });

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
            }
        }

        private void Export(object arg)
        {
            string[] arrArg = (string[])arg;
            string strMySqlConn = arrArg[0];
            string strOraConn = arrArg[1];
            string strOraUserName = arrArg[2];
            string strLogFile = arrArg[3];
            int iThreadCount = Convert.ToInt32(arrArg[4]);
            string strOraName = arrArg[5];
            int iBeginIndex = Convert.ToInt32(arrArg[6]);
            int iSingleTableCount = Convert.ToInt32(arrArg[7]);
            int lCount = 0;
            // File.Delete(strLogFile);
            DBHelper dbHelperOra = null;
            try
            {
                dbHelperOra = DBHelper.GetInstance("Oracle", strOraConn, false);
                lCount += dbHelperOra.GetCount(strOraName, "");
                ////mysql单独测试 
                //dbHelperOra = DBHelper.GetInstance("MySql", strMySqlConn, false);
                //lCount += dbHelperOra.GetCount("czrk17", "");
            }
            catch (System.Exception ex)
            {
                MessageBox.Show("Export:" + ex.ToString());
                return;
            }
            Dispatcher.Invoke(new Action(delegate { pbExport.Maximum = lCount; pbExport.Value = iBeginIndex - 1; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value, lCount); }));
            ExportTable(iThreadCount, strMySqlConn, strOraConn, strLogFile, lCount, strOraName, iBeginIndex, iSingleTableCount, isPrintSuccess);
            

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

        private void ExportTable(int ThreadCount, string MySqlConn, string OraConn, string LogFile, long Count, String strOraName, int iBeginIndex, int iSingleTableCount, bool isPrintSuccess)
        {
            try
            {
                int iLen = mQueryCount;
                DBHelper dbHelper = DBHelper.GetInstance("Oracle", OraConn, false);
                DBHelper dbHelperMySql = DBHelper.GetInstance("MySql", MySqlConn, false);
                int iCurIndex = iBeginIndex;
                String strMysqlTableName = "gxxyr1";
                int num = 0; 
                int lastNum = -1;
                while (true)
                {
                   
                    DataTable dtInfos = dbHelper.GetDataTable("xm,xb,zjhm,systemid", strOraName, "", "", "", iCurIndex, iLen);
                    ////mysql单独测试 
                    //DataTable dtInfos = dbHelperMySql.GetDataTable("id,xm,xb", "czrk17", "", "", "id", iCurIndex, iLen);
                    if (dtInfos == null)
                    {
                        break;
                    }
                    iCurIndex = iCurIndex + dtInfos.Rows.Count;
                    num = iCurIndex / iSingleTableCount;
                    if (num!=lastNum)
                    {
                        try
                        {
                            dbHelperMySql.ExecuteSql("CREATE TABLE IF NOT EXISTS " + "gxyxr" + (num + 1) + " LIKE czrk;");
                            strMysqlTableName = "gxxyr" + (num + 1);
                        }
                        catch (System.Exception ex)
                        {
                            MessageBox.Show(ex.ToString());
                        }                       
                    }
                    lastNum = num;
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
                    thExportTable.Start(new object[] { strMysqlTableName, MySqlConn, OraConn, LogFile, dtInfos, Count, strOraName, isPrintSuccess });
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("ExportTable:" + ex.ToString());
            }
        }

        private Thread GetThread()
        {
            Thread thCur = null;
            foreach (Thread thd in mThread)
            {
                if (thd.ThreadState == ThreadState.Stopped || thd.ThreadState == ThreadState.Unstarted)
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
            object[] arrArg = (object[])Arg;
            string strTable = (string)arrArg[0];
            string strMySqlConn = (string)arrArg[1];
            string strOraConn = (string)arrArg[2];
            string strFile = (string)arrArg[3];
            DataTable dtInfos = (DataTable)arrArg[4];
            long lCount = (long)arrArg[5];
            String strOraName = (string)arrArg[6];
            bool isPrintSuccess = (bool)arrArg[7];
            DBHelper dbHelperMySql = null;
            DBHelper dbHelper = null;
            DataTable dtOra = null;
            int photoNo = 0; //第几个照片是正面

            try
            {
                dbHelperMySql = DBHelper.GetInstance("MySql", strMySqlConn, false);
                dbHelper = DBHelper.GetInstance("Oracle", strOraConn, false);
            }
            catch (System.Exception ex)
            {
                MessageBox.Show("ExportThread" + ex.ToString());
                return;
            }
            if (dbHelper == null)
            {
                MessageBox.Show("实例化ORACLE数据库失败!");
                return;
            }
            //   DateTime dt1;
            //  DateTime dt2;
            Boolean bol = false;
            //  MessageBox.Show("本次加载了" + dtInfos.Rows.Count);
            //每一条mysql中的数据
            foreach (DataRow drInfo in dtInfos.Rows)
            {
                photoNo = 0;
                try
                {
                    if (drInfo["systemid"] == null || drInfo["systemid"].ToString() == "null" || drInfo["systemid"].ToString() == "")
                    {
                        bol = dbHelperMySql.Insert(strTable, new string[] { "xm", "xb", "zjhm", "csrq", "updatetime", "age" }, new object[] { drInfo["xm"], (drInfo["xb"].ToString() == "null" || drInfo["xb"].ToString() == "") ? 3 : drInfo["xb"], drInfo["zjhm"], "1975-01-01", DateTime.Now, 1975 });
                        if (!bol)
                        {
                            lock (mLock)
                            {
                                File.AppendAllText(strFile, string.Format("表{0}：pid为 {1} 更新失败，ZPID为空\r\n", strTable, drInfo["pid"]));
                                Dispatcher.Invoke(new Action(delegate { txtErrorCount.Text = (Convert.ToInt32(txtErrorCount.Text) + 1).ToString(); }));
                            }
                        }
                        else if (isPrintSuccess)
                        {
                            lock (mLock)
                            {
                                File.AppendAllText(strFile, string.Format("表{0}：pid为 {1} 更新成功，ZPID为空\r\n", strTable, drInfo["pid"]));
                            }
                        }
                        processUpdate(lCount);
                        continue;
                    }
                    //    dt1 = DateTime.Now;
                    //      File.AppendAllText(strFile, string.Format("准备读取照片编号为{0}的Oracle记录\r\n", drInfo["zpid"].ToString()));
                    //获取oracle人员信息表里的数据
                    dtOra = dbHelper.GetDataTable("select ywbbh,photo,photoname from V_RYZP where ywbbh='" + drInfo["systemid"].ToString() + "'");
                    if (dtOra == null)
                    {
                        processUpdate(lCount);
                        continue;
                    }
                    if (dtOra.Rows.Count>1)
                    {
                            for (int i = 0; i < dtOra.Rows.Count; i++)
                            {
                                if (dtOra.Rows[i]["photo"].ToString() == "1")
                                {
                                    photoNo = i;
                                }
                            }                    
                    }

                    bol = dbHelperMySql.Insert(strTable, new string[] { "xm", "xb", "zjhm", "csrq", "updatetime", "zpbh", "rxzp", "age" }, new object[] { drInfo["xm"], (drInfo["xb"].ToString() == "null" || drInfo["xb"].ToString() == "") ? 3 : drInfo["xb"], drInfo["zjhm"], "1975-01-01", DateTime.Now, drInfo["systemid"], dtOra.Rows[photoNo]["photo"], 1975 });
                    //     MessageBox.Show("更新MySql用时：" + (DateTime.Now - dt2).TotalSeconds.ToString());
                    if (!bol)
                    {
                        lock (mLock)
                        {
                            File.AppendAllText(strFile, string.Format("表{0}：systemid为 {1} 更新失败\r\n", strTable, drInfo["systemid"]));
                            Dispatcher.Invoke(new Action(delegate { txtErrorCount.Text = (Convert.ToInt32(txtErrorCount.Text) + 1).ToString(); }));
                        }
                    }
                    else if (isPrintSuccess)
                    {
                        lock (mLock)
                        {
                            File.AppendAllText(strFile, string.Format("表{0}：systemid为 {1} 更新成功\r\n", strTable, drInfo["systemid"]));
                        }
                    }
                    ////mysql单独测试 
                    //dbHelperMySql.Insert(strTable, new string[] { "xm", "xb" }, new object[] { drInfo["xm"], drInfo["xb"] });
                    //if (isPrintSuccess)
                    //{
                    //    lock (mLock)
                    //    {
                    //        File.AppendAllText(strFile, string.Format("表{0}：ID为 {1} 更新成功\r\n", strTable, drInfo["id"]));
                    //    }
                    //}
                    processUpdate(lCount);
                }
                catch (System.Exception ex)
                {
                    MessageBox.Show("ExportThread" + ex.ToString());
                    processUpdate(lCount);
                    continue;
                }
            }
            dtInfos.Dispose();
            dtInfos = null;
            if (dtOra != null)
            {
                dtOra.Dispose();
            }
        }



        public void processUpdate(long lCount)
        {
            lock (mLockPB)
            {
                Dispatcher.Invoke(new Action(delegate
                {
                    pbExport.Value = pbExport.Value + 1; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value, lCount);
                    if (pbExport.Value == lCount)
                    {
                        MessageBox.Show("导出完成！");
                        isComplete = true;
                        return;
                    }
                }));
            }
        }
    }
}