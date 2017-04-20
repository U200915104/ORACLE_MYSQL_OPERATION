using System;
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
    public partial class UpdateMySqlFromOracle : Window
    {
        private static object mLock = new object();
        private static object mLockPB = new object();
        private static Boolean isComplete = true;
        private List<Thread> mThread;
        private int mThreadCount;
        private string mServerIP;
        private int mQueryCount = 2000;
        public UpdateMySqlFromOracle()
        {
            InitializeComponent();

            txtAddr.Text = "10.24.107.140";
            txtPort.Text = "3306";
            txtUserName.Text = "root";
            txtPassword.Text = "root";
            txtDBName.Text = "faceserverdb";

            txtAddrTo.Text = "10.24.107.163";
            txtPortTo.Text = "1521";
            txtUserNameTo.Text = "rksj";
            txtPasswordTo.Text = "rksj";
            txtOraService.Text = "orcl";
            txtOraTable.Text = "";
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

                mServerIP = txtAddr.Text.Trim();
                mThreadCount = Convert.ToInt32(txtThreadCount.Text.Trim());
                mThread = new List<Thread>(mThreadCount);
                mQueryCount = Convert.ToInt32(txtQueryCount.Text.Trim());
                // return;
                Thread thExport = new Thread(new ParameterizedThreadStart(Export));
                thExport.Start(new string[] { strMySqlConn, strOraConn, txtUserNameTo.Text.Trim(), txtFileName.Text.Trim(), txtThreadCount.Text.Trim(), txtOraTable.Text.Trim() });

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
            string strOraTBName = arrArg[5];
            int lCount = 0;
            File.Delete(strLogFile);
            DBHelper dbHelperMySql = null;
            DataTable dtMysqlTbInfos = null;
            try
            {
                dbHelperMySql = DBHelper.GetInstance("MySql", strMySqlConn, false);
                dtMysqlTbInfos = dbHelperMySql.GetDataTable("select id,name from tableinfo");
                if (dtMysqlTbInfos == null)
                {
                    MessageBox.Show("找不到mysql表！");
                    return;
                }
                foreach (DataRow drInfo in dtMysqlTbInfos.Rows)
                {
                    lCount += dbHelperMySql.GetCount(drInfo["name"].ToString(), "");
                }
                Dispatcher.Invoke(new Action(delegate { pbExport.Maximum = lCount; tbProcess.Text = string.Format("0/{0}", lCount); }));
                ExportTable(dtMysqlTbInfos, iThreadCount, strMySqlConn, strOraConn, strLogFile, lCount, strOraTBName);
            }
            catch (System.Exception ex)
            {
                MessageBox.Show("Export:" + ex.ToString());
                return;
            }
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

        private void ExportTable(DataTable dataTable, int ThreadCount, string MySqlConn, string OraConn, string LogFile, long Count, string strOraTBName)
        {
            try
            {
                int iLen = mQueryCount;
                DBHelper dbHelperMySql = DBHelper.GetInstance("MySql", MySqlConn, false);
                foreach (DataRow drInfo in dataTable.Rows)
                {
                    int iCurIndex = 1;
                    while (true)
                    {
                        DataTable dtInfos = new DataTable();
                        dtInfos = dbHelperMySql.GetDataTable("id,rybh,zpbh,facetype", drInfo["name"].ToString(), "", "", "id", iCurIndex, iLen);
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
                        thExportTable.Start(new object[] { drInfo["name"].ToString(), MySqlConn, OraConn, LogFile, dtInfos, Count, strOraTBName });
                    }
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
            string strOraTBName = (string)arrArg[6];
            DBHelper dbHelperMySql = null;
            DBHelper dbHelper = null;
            try
            {
                dbHelperMySql = DBHelper.GetInstance("MySql", strMySqlConn, false);
                dbHelper = DBHelper.GetInstance("Oracle", strOraConn, false);

                DataTable dtOra = null;
                DateTime dt1;
                DateTime dt2;
                DateTime dt3;
                DateTime dt4;
                Boolean bol = false;
                //  MessageBox.Show("本次加载了" + dtInfos.Rows.Count);
                foreach (DataRow drInfo in dtInfos.Rows)
                {

                    if (drInfo["zpbh"] == null || drInfo["facetype"].ToString().Equals("1") || drInfo["zpbh"].ToString().ToLower() == "null" || drInfo["zpbh"].ToString().Length == 0)
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
                        continue;
                    }
                    dt1 = DateTime.Now;
                    try
                    {
                        dtOra = dbHelper.GetDataTable("select RNO,ZPXLH,MZ from " + strOraTBName + " where ZPXLH=" + drInfo["zpbh"].ToString());
                    }
                    catch (System.Exception ex)
                    {
                        MessageBox.Show("ExportThread" + ex.ToString());
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
                        continue;
                    }
                   
                    dt2 = DateTime.Now;
                    // lock (mLock)
                    // {
                    //     File.AppendAllText(strFile, string.Format("读取ZPXLH为{0}的Oracle记录用时{1}秒\r\n", drInfo["zpbh"].ToString(), (dt2-dt1).TotalSeconds));
                    // }
                    if (dtOra == null)
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
                        continue;
                    }   
                    dt3 = DateTime.Now;
                    try
                    {
                        bol = dbHelperMySql.Update(strTable, "zpbh='" + dtOra.Rows[0]["ZPXLH"] + "'", new string[] { "rybh", "facetype", "mz" }, new object[] { dtOra.Rows[0]["RNO"], 1, dtOra.Rows[0]["MZ"] });
                    }
                    catch (System.Exception ex)
                    {
                        MessageBox.Show("ExportThread" + ex.ToString());
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
                        continue;
                    }
                    dt4 = DateTime.Now;
                    // lock (mLock)
                   // {
                   //     File.AppendAllText(strFile, string.Format("更新zpbh为{0}的MySql记录用时{1}秒\r\n", dtOra.Rows[0]["ZPXLH"].ToString(), (dt4-dt3).TotalSeconds));
                   // }
                    if (!bol)
                    {
                        lock (mLock)
                        {
                            File.AppendAllText(strFile, string.Format("照片编号：{0}更新失败\r\n", drInfo["zpbh"]));
                            Dispatcher.Invoke(new Action(delegate { txtErrorCount.Text = (Convert.ToInt32(txtErrorCount.Text) + 1).ToString(); }));
                        }
                    }
                }
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
            catch (System.Exception ex)
            {
                MessageBox.Show("ExportThread" + ex.ToString());
            }

            dtInfos.Dispose();
            dtInfos = null;
        }

    }
}
