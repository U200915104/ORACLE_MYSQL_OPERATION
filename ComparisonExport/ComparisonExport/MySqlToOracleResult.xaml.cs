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
using Microsoft.Win32;
using System.Threading;
using DAL;
using System.Data;
using System.IO;

namespace ComparisonExport
{
    /// <summary>
    /// MySqlToOracleResult.xaml 的交互逻辑
    /// </summary>
    public partial class MySqlToOracleResult : Window
    {
        private static object mLock = new object();
        private static object mLockPB = new object();
        private Dictionary<int, DBHelper> mDBHelperServer;
        private Dictionary<int, string> mTBNameServer;
        private Dictionary<int, int> mTBServerID;
        private List<Thread> mThread;
        private int mThreadCount;
        private int mQueryCount;
        public MySqlToOracleResult()
        {
            InitializeComponent();

            txtAddr.Text = "10.2.64.15";
            txtPort.Text = "3306";
            txtUserName.Text = "root";
            txtPassword.Text = "root";
            txtDBName.Text = "facecomparisondb";

            txtAddr1.Text = "10.2.64.15";
            txtPort1.Text = "3306";
            txtUserName1.Text = "root";
            txtPassword1.Text = "root";
            txtDBName1.Text = "faceserverdb";

            txtAddrTo.Text = "10.2.64.247";
            txtPortTo.Text = "1521";
            txtUserNameTo.Text = "tckts";
            txtPasswordTo.Text = "tckts";
            txtOraService.Text = "rxbd2";
            txtFileName.Text = "d:\\logfile.txt";
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

        private void btnOK_Click(object sender, RoutedEventArgs e)
        {
            string strMySqlComConn = string.Format("Data Source={0};Database={1};User Id={2};Password={3};pooling=true;CharSet=utf8;port={4};", txtAddr.Text.Trim(),
              txtDBName.Text.Trim(), txtUserName.Text.Trim(), txtPassword.Text.Trim(), txtPort.Text.Trim());
            string strMySqlServerConn = string.Format("Data Source={0};Database={1};User Id={2};Password={3};pooling=true;CharSet=utf8;port={4};", txtAddr1.Text.Trim(),
              txtDBName1.Text.Trim(), txtUserName1.Text.Trim(), txtPassword1.Text.Trim(), txtPort1.Text.Trim());
            string strOraConn = string.Format("Provider=OraOLEDB.Oracle.1;User ID={0};Password={1};Data Source=(DESCRIPTION = (ADDRESS_LIST= (ADDRESS = (PROTOCOL = TCP)(HOST = {2})(PORT = {3}))) (CONNECT_DATA = (SERVICE_NAME = {4})))", txtUserNameTo.Text.Trim(),
                txtPasswordTo.Text.Trim(), txtAddrTo.Text.Trim(), txtPortTo.Text.Trim(), txtOraService.Text.Trim());

            mThreadCount = Convert.ToInt32(txtThreadCount.Text.Trim());
            mThread = new List<Thread>(mThreadCount);
            mQueryCount = Convert.ToInt32(txtQueryCount.Text.Trim());
            Thread thExport = new Thread(new ParameterizedThreadStart(Export));
            thExport.Start(new string[] { strMySqlServerConn,strMySqlComConn, strOraConn, txtUserNameTo.Text.Trim(), txtFileName.Text.Trim() });

        }

        private void Export(object Arg)
        {
            int iLen = mQueryCount;
            string[] arrArg = (string[])Arg;
            string strServerConn = arrArg[0];
            string strComConn = arrArg[1];
            string strOraConn = arrArg[2];
            string strOraUserName = arrArg[3];
            string strLogFile = arrArg[4];

            DBHelper dbHelperServer = DBHelper.GetInstance("Mysql", strServerConn, false);
            DBHelper dbHelperCom = DBHelper.GetInstance("Mysql", strComConn, false);
            DBHelper dbHelperOra = DBHelper.GetInstance("Oracle", strOraConn, false);


            DataTable dtTbInfo = dbHelperServer.GetDataTable("id,name,serverid,serverip", "tableinfo");
            if (dtTbInfo == null)
            {
                MessageBox.Show("获取表信息失败！");
                return;
            }

            mDBHelperServer = new Dictionary<int, DBHelper>();
            mTBNameServer = new Dictionary<int, string>();
            mTBServerID = new Dictionary<int, int>();

            int iTbID;
            int iServerID;
            string strServerIP;
            string strTBName;
            foreach (DataRow drTb in dtTbInfo.Rows)
            {
                iTbID = (int)drTb["id"];
                iServerID = (int)drTb["serverid"];

                if (!mTBServerID.ContainsKey(iTbID))
                {
                    mTBServerID.Add(iTbID, iServerID);                    
                }

                if (!mTBNameServer.ContainsKey(iTbID))
                {
                    strTBName = (string)drTb["name"];
                    mTBNameServer.Add(iTbID, strTBName);
                }

                if (!mDBHelperServer.ContainsKey(iServerID))
                {
                    strServerIP = (string)drTb["serverip"];
                    mDBHelperServer.Add(iServerID, DBHelper.GetInstance("MySql", "faceserverdb", strServerIP, "3306", "root", "root"));
                }
            }

            //if (dbHelperOra.GetCount("dba_tables", string.Format("owner = '{0}' and table_name = 'T_PC_BDJG'", strOraUserName.ToUpper())) == 0)
            //{
            //    string strSql = string.Format("CREATE TABLE {0}.T_PC_BDJG (\"BDJGID\" NUMBER(10), \"TCID\" NUMBER(10),\"CSID\" NUMBER(10),\"XSD\" NUMBER(10,5), \"TCPX\" NUMBER(2))", strOraUserName.ToUpper());
            //    // string strSql = "CREATE TABLE 'VIEW_PC_MB' ('MBID' NUMBER NOT NULL ENABLE, 'MBA' VARCHAR2(20 BYTE),'MBB' BLOB)";
            //    if (dbHelperOra.ExecuteSql(strSql) < 0)
            //    {
            //        MessageBox.Show("创建T_PC_BDJG失败！");
            //        return;
            //    }
            //}

            int iCount = dbHelperCom.GetCount("comparisoninfo", "flag=0");
            if (iCount == 0)
            {
                MessageBox.Show("导出完成！");
                return;
            }
            Dispatcher.Invoke(new Action(delegate { pbExport.Maximum = iCount; tbProcess.Text = string.Format("0/{0}", iCount); }));

            int iCurIndex = 1;
            while (true)
            {
                DataTable dtInfos = dbHelperCom.GetDataTable("id,note,resultids,flag", "comparisoninfo", "", "id", "id", iCurIndex, iLen);
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
                thExportTable.Start(new object[] { dtInfos, strOraConn, strLogFile, iCount, strComConn });

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
            object[] arrArg=(object[])Arg;
            DataTable dtInfo = (DataTable)arrArg[0];
            string strOraConn = (string)arrArg[1];
            string strLogFile = (string)arrArg[2];
            int iCount = (int)arrArg[3];
            string strComConn = (string)arrArg[4];

            string strTCID;
            string strCXID;
            string strResultIDs;
            string[] arrResultIDs;
            string strResultID;
            string[] arrIDs;
            string strTBName;
            int iTBID;
            int iPersonID;
            double dScore;

            DataTable dtZJHM;
            int iIndex = 1;
            int iComID;
            DBHelper dbHelper = DBHelper.GetInstance("Oracle", strOraConn, false);
            DBHelper dbHelperCom = DBHelper.GetInstance("MySql", strComConn, false);
            DBHelper dbHelperSer;
            foreach (DataRow drInfo in dtInfo.Rows)
            {
                if ((int)drInfo["flag"] == 1)
                {
                    continue;
                }
                if (drInfo["note"] == System.DBNull.Value)
                {
                    continue;
                }
                if (drInfo["resultids"] == System.DBNull.Value)
                {
                    continue;
                }
                iComID=(int)drInfo["id"];
                strTCID = (string)drInfo["note"];
                strResultIDs = (string)drInfo["resultids"];
                if (string.IsNullOrEmpty(strResultIDs))
                {
                    continue;
                }
                arrResultIDs = strResultIDs.Split(';');
                iIndex = 1;
                for (int i = 0; i < 20&&i<arrResultIDs.Length; i++)
                {
                    strResultID = arrResultIDs[i];
                    arrIDs = strResultID.Split(',');
                    iTBID=Convert.ToInt32(arrIDs[0]);
                    iPersonID = Convert.ToInt32(arrIDs[1]);
                    dScore = Convert.ToDouble(arrIDs[2])*100;
                    if (!mTBNameServer.ContainsKey(iTBID))
                    {
                        continue;
                    }
                    dbHelperSer=mDBHelperServer[mTBServerID[iTBID]];
                    strTBName = mTBNameServer[iTBID];

                    dtZJHM = dbHelperSer.GetDataTable("zjhm", strTBName, string.Format("id={0}", iPersonID));
                    if (dtZJHM == null)
                    {
                        continue;
                    }
                    strCXID=(string)dtZJHM.Rows[0]["zjhm"];
                    if (!dbHelper.Insert("T_PC_BDJG", new string[] { "BDJGID", "TCID", "CSID", "XSD", "TCPX" }, new object[] { iComID, strTCID, strCXID, dScore, iIndex }))
                    {
                        lock (mLock)
                        {
                            File.AppendAllText(strLogFile, string.Format("{0}-{1}-{2}\r\n", iComID, strTCID, strCXID));
                            Dispatcher.Invoke(new Action(delegate { txtErrorCount.Text = (Convert.ToInt32(txtErrorCount.Text) + 1).ToString(); }));
                        }
                    }
                    dbHelperCom.Update("comparisoninfo", string.Format("id={0}", iComID), new string[] { "flag" }, new object[] { 1 });
                    lock (mLockPB)
                    {
                        Dispatcher.Invoke(new Action(delegate
                        {
                            pbExport.Value = pbExport.Value + 1; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value, iCount);
                            if (pbExport.Value == iCount)
                            {
                                MessageBox.Show("导出完成！");
                                return;
                            }
                        }));

                    }

                    iIndex++;
                }
            }
        }
    }
}
