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
using System.IO;
using System.Net;
using DAL;
using System.Data;
using Microsoft.Win32;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Timers;
namespace ComparisonExport
{
    /// <summary>
    /// OracleToMySql.xaml 的交互逻辑
    /// </summary>
    public partial class OracleToMySql1 : Window
    {
        private MyTimer aTimer = null;
        private DateTime lastDateTime = DateTime.Now;
        private static object mLock = new object();
        private static object mLockPB = new object();
        private List<Thread> mThread;
        private int mThreadCount = 1;
        private int mQueryCount = 2000;
        private int mTotalTables = 0;
        private int mFinshTables = 0;
        public OracleToMySql1()
        {
            InitializeComponent();

            txtAddrTo.Text = "10.10.181.138";
            //txtAddrTo.Text = "10.24.107.163";
            txtPortTo.Text = "1521";
            //txtUserNameTo.Text = "rksj";
            //txtPasswordTo.Text = "rksj";
            //txtOraService.Text = "orcl";
            txtUserNameTo.Text = "viot";
            txtPasswordTo.Text = "viot";
            txtOraService.Text = "viot";
            txtOraTables.Text = "HB_RK_ZPXX,HB_RK_2,HB_RK_3,HB_RK_4,HB_RK_5,HB_RK_6,HB_RK_7,HB_RK_8";
            txtOraDate.Text = "2017-03-21";
            txtFileDirecory.Text = "d:/1/";
            // txtZJMBurl.Text = "http://10.24.107.153:8080/BatchAddFaceInfo";
            txtZJMBurl.Text = "http://10.10.181.138:8080/FaceComparison_hebei/BatchAddFaceInfo";

            tbkConsole.Text = "";

            aTimer = new MyTimer();
            aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
            aTimer.Interval = 10000;
            aTimer.Enabled = false;
        }

        private void btnOK_Click(object sender, RoutedEventArgs e)
        {
            btnOK.IsEnabled = false;
            //mThreadCount = Convert.ToInt32(txtThreadCount.Text.Trim());
            mThread = new List<Thread>(mThreadCount);
            //mQueryCount = Convert.ToInt32(txtQueryCount.Text.Trim());
            if (!System.IO.Directory.Exists(txtFileDirecory.Text.Trim()))
            {
                tbkConsole.Text += "[" + DateTime.Now.ToString() + "][error]日志目录不是一个文件夹路径!\r\n";
                btnOK.IsEnabled = true;
                return;
            }

            string strOraConn = string.Format("Provider=OraOLEDB.Oracle.1;User ID={0};Password={1};Data Source=(DESCRIPTION = (ADDRESS_LIST= (ADDRESS = (PROTOCOL = TCP)(HOST = {2})(PORT = {3}))) (CONNECT_DATA = (SERVICE_NAME = {4})))", txtUserNameTo.Text.Trim(),
                txtPasswordTo.Text.Trim(), txtAddrTo.Text.Trim(), txtPortTo.Text.Trim(), txtOraService.Text.Trim());
            aTimer.strOraConn = strOraConn;
            aTimer.txtOraDate = txtOraDate.Text.Trim();
            aTimer.txtOraTables = txtOraTables.Text.Trim();
            aTimer.txtFileDirecory = txtFileDirecory.Text.Trim();
            aTimer.txtZJMBurl = txtZJMBurl.Text.Trim();

            Thread thExport = new Thread(new ParameterizedThreadStart(Synchronize));
            thExport.Start(new string[] { strOraConn, txtOraTables.Text.Trim(), txtOraDate.Text.Trim(), txtZJMBurl.Text.Trim(), txtFileDirecory.Text.Trim() });

        }

        private void Synchronize(object arg)
        {
            //aTimer.Start();
            string[] arrArg = (string[])arg;
            string strOraConn = arrArg[0];
            string strOraTables = arrArg[1];
            string strOraDate = arrArg[2];
            string strZJMBurl = arrArg[3];
            string strFileDirecory = arrArg[4];

            //File.Delete(strLogFile);
            DBHelper dbHelperOra = DBHelper.GetInstance("Oracle", strOraConn, false);
            Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]正在测试ORACLE连接，请稍后...\r\n"; }));
            if (!dbHelperOra.IsConnected())
            {
                Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][error]未连接到ORACLE数据库!\r\n"; }));
                Dispatcher.Invoke(new Action(delegate { btnOK.IsEnabled = true; }));
                return;
            }
            else
            {
                Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]连接ORACLE数据库成功!\r\n"; }));
            }
            Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]正在测试web服务器连接，请稍后...\r\n"; }));
            String ret = SendHttp(strZJMBurl, "testConnect", 0, null, null, null, null, null, null, null, null);
            if (ret != "connect")
            {
                //Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][error]与web服务器通信异常!\r\n"; }));
                Dispatcher.Invoke(new Action(delegate { btnOK.IsEnabled = true; }));
                return;
            }
            else
            {
                Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]连接web服务成功!\r\n"; }));
            }

            aTimer.Enabled = true;
            //aTimer.Start();
        }

        private void OnTimedEvent(object sender, EventArgs e)
        {
            aTimer.Stop();
            if (lastDateTime.DayOfYear != DateTime.Now.DayOfYear)
            {
                Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]开始同步数据!\r\n"; }));
                SynchronizeOraTable(aTimer.txtOraTables.ToString(), 1, aTimer.txtZJMBurl.ToString(), aTimer.strOraConn.ToString(), aTimer.txtFileDirecory.ToString(), aTimer.txtOraDate.ToString());


            }

        }

        private void SynchronizeOraTable(string OraTables, int ThreadCount, string ZJMBurl, string OraConn, string FileDirecory, string OraDate)
        {
            string strFile = System.IO.Path.Combine(FileDirecory, DateTime.Now.ToString("yyyy-MM-dd") + ".txt");
            int iLen = mQueryCount;
            DBHelper dbHelper = DBHelper.GetInstance("Oracle", OraConn, false);
            int iCurIndex = 1;
            int SynCount = 0;
            Boolean isLastGroup = false;
            string[] arrTables = OraTables.Split(',');
            mTotalTables = arrTables.Length;
            mFinshTables = 0;
            foreach (string table in arrTables)
            {
                Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]开始同步表" + table + "...\r\n"; }));
                lock (mLock)
                {
                    File.AppendAllText(strFile, string.Format("[" + DateTime.Now.ToString() + "][info]开始同步表" + table + "...\r\n"));
                }
                try
                {
                    SynCount = dbHelper.GetCount(table, "tbsj>to_date('" + OraDate + "','yyyy-mm-dd') order by tbsj desc ");
                }
                catch (System.Exception ex)
                {
                    Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][error]读取表" + table + "失败！\r\n"; }));
                    lock (mLock)
                    {
                        File.AppendAllText(strFile, string.Format("[" + DateTime.Now.ToString() + "][error]读取表" + table + "失败！\r\n"));
                        mFinshTables++;
                    }
                    
                    continue;
                }
                DataTable dtInfos = new DataTable();
                while (true)
                {
                    try
                    {
                        dtInfos = dbHelper.GetDataTable("rno,xm,xb,gmsfhm,zpxlh,csrq,ssxq,xp,tbsj", table, "tbsj>to_date('" + OraDate + "','yyyy-mm-dd')", "tbsj desc", "", iCurIndex, iLen);
                    }
                    catch (System.Exception ex)
                    {
                        //MessageBox.Show("ExportThread" + ex.ToString());
                        Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][error]读取表" + table + "失败！\r\n"; }));
                        lock (mLock)
                        {
                            File.AppendAllText(strFile, string.Format("[" + DateTime.Now.ToString() + "][error]读取表" + table + "失败！\r\n"));
                            mFinshTables++;
                        }                      
                        break;
                    }

                    if (dtInfos == null)
                    {
                        break;
                    }
                    iCurIndex = iCurIndex + dtInfos.Rows.Count;
                    if (iCurIndex >= SynCount)
                    {
                        isLastGroup = true;
                    }
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
                    thExportTable.Start(new object[] { table, OraConn, strFile, dtInfos, ZJMBurl, isLastGroup });

                }
                continue;
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
            string strOraConn = (string)arrArg[1];
            string strFile = (string)arrArg[2];
            DataTable dtInfos = (DataTable)arrArg[3];
            string strZJMBurl = (string)arrArg[4];
            Boolean isLastGroup = (Boolean)arrArg[5];
            int dbid = 0;

            if (strTable == "HB_RK_ZPXX") dbid = 1;
            else if (strTable == "HB_RK_2") dbid = 2;
            else if (strTable == "HB_RK_3") dbid = 3;
            else if (strTable == "HB_RK_4") dbid = 4;
            else if (strTable == "HB_RK_5") dbid = 5;
            else if (strTable == "HB_RK_6") dbid = 6;
            else if (strTable == "HB_RK_7") dbid = 7;
            else if (strTable == "HB_RK_8") dbid = 8;
            else return;
            string strRet = null;
            foreach (DataRow drInfo in dtInfos.Rows)
            {
                try
                {
                    strRet = SendHttp(strZJMBurl, "addFaceInfo", dbid, drInfo["RNO"].ToString(), drInfo["XM"].ToString(), drInfo["XB"].ToString(), drInfo["GMSFHM"].ToString(), drInfo["ZPXLH"].ToString(), drInfo["CSRQ"].ToString(), drInfo["SSXQ"].ToString(), (byte[])drInfo["XP"]);
                }
                catch (System.Exception ex)
                {
                    MessageBox.Show(ex.ToString());
                    return;
                }

                if (strRet != "0")
                {
                    lock (mLock)
                    {
                        File.AppendAllText(strFile, string.Format("[" + DateTime.Now.ToString() + "][error]同步" + strTable + " -- RNO:" + drInfo["RNO"] + " -- ZPLXH:" + drInfo["ZPXLH"] + " -- TBSJ:" + drInfo["TBSJ"] + "失败！！！\r\n"));
                    }
                }
                else
                {
                    lock (mLock)
                    {
                        File.AppendAllText(strFile, string.Format("[" + DateTime.Now.ToString() + "][error]同步" + strTable + " -- RNO:" + drInfo["RNO"] + " -- ZPLXH:" + drInfo["ZPXLH"] + " -- TBSJ:" + drInfo["TBSJ"] + "成功！\r\n"));
                    }
                }
            }
            dtInfos.Dispose();
            dtInfos = null;
            if (isLastGroup)
            {
                Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]表" + strTable + "同步完成!\r\n"; }));
                lock (mLock)
                {
                    mFinshTables++;
                    if (mFinshTables == mTotalTables)
                    {
                        Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][info]本次同步完成!请等待下次同步自动执行!详情请查看日志！\r\n"; }));
                        File.AppendAllText(strFile, string.Format("[" + DateTime.Now.ToString() + "][info]本次同步完成!请等待下次同步自动执行!\r\n"));
                        aTimer.Start();
                        //Dispatcher.Invoke(new Action(delegate { btnOK.IsEnabled = true; }));
                    }
                }
            }
            

        }

        private String SendHttp(String url, String action, int dbid, String rno, String xm, String xb, String gmsfhm, String zpxlh, String csrq, String ssxq, byte[] xp)
        {
            //发送准备
            JObject job = new JObject();
            if (action == "testConnect")
            {
                job.Add("action", action);
            }
            else
            {
                job.Add("action", action);
                job.Add("dbid", dbid);
                job.Add("rno", rno);
                job.Add("xm", xm);
                job.Add("xb", xb);
                job.Add("gmsfhm", gmsfhm);
                job.Add("zpxlh", zpxlh);
                job.Add("csrq", csrq);
                job.Add("ssxq", ssxq);
                job.Add("xp", PictureToBase64(xp));
            }
            byte[] data = ASCIIEncoding.UTF8.GetBytes(job.ToString());
            string strUri = url;
            //发送请求
            try
            {
                HttpWebRequest myRequest = (HttpWebRequest)WebRequest.Create(strUri);
                myRequest.Method = "POST";
                myRequest.ContentType = "application/octet-stream";
                myRequest.ContentLength = data.Length;
                Stream newStream = myRequest.GetRequestStream();
                newStream.Write(data, 0, data.Length);
                newStream.Flush();
                newStream.Close();
                // Get response  
                HttpWebResponse myResponse = (HttpWebResponse)myRequest.GetResponse();
                StreamReader reader = new StreamReader(myResponse.GetResponseStream(), Encoding.Default);
                string content = reader.ReadToEnd();
                return content;
            }
            catch (System.Exception ex)
            {
                Dispatcher.Invoke(new Action(delegate { tbkConsole.Text += "[" + DateTime.Now.ToString() + "][error]与web服务器通信异常!\r\n"; }));
                return "-11";
            }


        }


        public static String PictureToBase64(byte[] inData)
        {
            try
            {
                string strPath = Convert.ToBase64String(inData, 0, inData.Length);
                return strPath;
            }
            catch (Exception e)
            {

            }
            return null;
        }
    }

    public class MyTimer : System.Timers.Timer
    {
        public object strOraConn { get; set; }
        public object txtOraDate { get; set; }
        public object txtOraTables { get; set; }
        public object txtFileDirecory { get; set; }
        public object txtZJMBurl { get; set; }

    }
}
