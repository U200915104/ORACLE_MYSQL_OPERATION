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
    public partial class UpdateMySqlFromOracle1 : Window
    {
        private static object mLock = new object();
        private static object mLockPB = new object();
        private static Boolean isComplete = true;
        private List<Thread> mThread;
        private int mThreadCount;
        private string mServerIP;
        private int mQueryCount = 2000;
        public UpdateMySqlFromOracle1()
        {
            InitializeComponent();

            txtAddr.Text = "127.0.0.1";
            txtPort.Text = "3306";
            txtUserName.Text = "root";
            txtPassword.Text = "root";
            txtDBName.Text = "faceserverdb_jiangxi";

            txtAddrTo.Text = "192.168.1.75";
            txtPortTo.Text = "1521";
            txtUserNameTo.Text = "rxbd";
            txtPasswordTo.Text = "manager";
            txtOraService.Text = "Racdb";
            txtOraTBName.Text = "RKJBXX_5101";

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
                thExport.Start(new string[] { strMySqlConn, strOraConn, txtUserNameTo.Text.Trim(), txtFileName.Text.Trim(), txtThreadCount.Text.Trim(), txtOraTBName.Text.Trim() });

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
            int lCount = 0;
           // File.Delete(strLogFile);
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
                ExportTable(dtMysqlTbInfos, iThreadCount, strMySqlConn, strOraConn, strLogFile, lCount, strOraName);
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

        private void ExportTable(DataTable dataTable, int ThreadCount, string MySqlConn, string OraConn, string LogFile, long Count, String strOraName)
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
                        dtInfos = dbHelperMySql.GetDataTable("id,xm,zjhm,rxzp,facetype", drInfo["name"].ToString(), "", "", "id", iCurIndex, iLen);

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
                        thExportTable.Start(new object[] { drInfo["name"].ToString(), MySqlConn, OraConn, LogFile, dtInfos, Count, strOraName });
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
            String strOraName = (string)arrArg[6];
            DBHelper dbHelperMySql = null;
            DBHelper dbHelper = null;
            DataTable dtOra = null;
            DataTable dtOraZP = null;
            try
            {
                dbHelperMySql = DBHelper.GetInstance("MySql", strMySqlConn, false);
                dbHelper = DBHelper.GetInstance("Oracle", strOraConn, false);
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

                    if (drInfo["zjhm"].ToString().StartsWith("0") || drInfo["facetype"].ToString() == "0")
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
                    //    dt1 = DateTime.Now;
                    //      File.AppendAllText(strFile, string.Format("准备读取ID为{0},照片编号为{1}的Oracle记录\r\n", drInfo["id"].ToString(), drInfo["zpbh"].ToString()));
                    //获取oracle人员信息表里的数据
                    dtOra = dbHelper.GetDataTable("select PERSONPK,SFZHM,RYZT from " + strOraName + " where SFZHM='" + drInfo["zjhm"].ToString() + "' and XM='" + drInfo["xm"].ToString()  + "'");
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

                    //找对应oracle人像照片表里的数据
                    String strOraZPName = strOraName.Replace("JBXX", "XP");
                    foreach (DataRow drOraInfo in dtOra.Rows)
                    {
                        dtOraZP = dbHelper.GetDataTable("select XPBH,XP,GMSFHM,PERSONPK from " + strOraZPName + " where PERSONPK='" + drOraInfo["PERSONPK"].ToString() + "'");
                        if (dtOraZP == null)
                        {
                            continue;
                        }
                        foreach (DataRow drOraZPInfo in dtOraZP.Rows)
                        {
                            if (((byte[])drInfo["rxzp"]).Length == ((byte[])drOraZPInfo["XP"]).Length)
                            {
                                //     dt2 = DateTime.Now;
                                //     MessageBox.Show("查oracle用时：" + (dt2 - dt1).TotalSeconds.ToString());

                                //lock (mLock)
                                //{
                                //    File.AppendAllText(strFile, string.Format("准备更新表{0}：ID为 {1} 更新 rybh:{2}--zpbh:{3}\r\n", strTable, drInfo["id"], drOraZPInfo["XPBH"].ToString(), drOraInfo["PERSONPK"].ToString()));
                                //}
                                
                                bol = dbHelperMySql.Update(strTable, "id=" + drInfo["id"].ToString(), new string[] { "txsm", "facetype", "zpbh", "code" }, new object[] { drOraZPInfo["XPBH"], 0, drOraInfo["PERSONPK"], drOraInfo["RYZT"].ToString() == "1" ? 0 : 1 });
                                //     MessageBox.Show("更新MySql用时：" + (DateTime.Now - dt2).TotalSeconds.ToString());
                                if (!bol)
                                {
                                    lock (mLock)
                                    {
                                        File.AppendAllText(strFile, string.Format("表{0}：ID为 {1} 更新失败\r\n", strTable, drInfo["id"]));
                                        Dispatcher.Invoke(new Action(delegate { txtErrorCount.Text = (Convert.ToInt32(txtErrorCount.Text) + 1).ToString(); }));
                                    }
                                }
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
            }
            catch (System.Exception ex)
            {
                MessageBox.Show("ExportThread" + ex.ToString());
                dtInfos.Dispose();
                dtInfos = null;
                if (dtOra!=null)
                {
                    dtOra.Dispose();
                }
                if (dtOra != null)
                {
                    dtOraZP.Dispose();
                }
            }

            dtInfos.Dispose();
            dtInfos = null;
            if (dtOra != null)
            {
                dtOra.Dispose();
            }
            if (dtOra != null)
            {
                dtOraZP.Dispose();
            }
        }

    }
}
