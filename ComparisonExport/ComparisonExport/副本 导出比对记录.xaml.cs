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
using System.Windows.Navigation;
using System.Windows.Shapes;
using DAL;
using System.Data;
using System.IO;
using Microsoft.Win32;
using System.Threading;
using System.Windows.Threading;

namespace ComparisonExport
{
    /// <summary>
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow2 : Window
    {

        private string mConnection = "Data Source={0};Database={1};User Id={2};Password={3};pooling=true;CharSet=utf8;port={4};";

        public MainWindow2()
        {
            InitializeComponent();

        }


        private void btnBrower_Click(object sender, RoutedEventArgs e)
        {
            SaveFileDialog sfd = new SaveFileDialog();
            sfd.Filter = "CSV文件(*.csv)|*.csv";

            if (sfd.ShowDialog() != true)
            {
                return;
            }

            txtFileName.Text = sfd.FileName;
        }

        private void btnExport_Click(object sender, RoutedEventArgs e)
        {


            string strAddr = txtAddr.Text.Trim();
            string strPort = txtPort.Text.Trim();
            string strUserName = txtUserName.Text.Trim();
            string strPassword = txtPassword.Text.Trim();
            string strFileName = txtFileName.Text.Trim();
            string strDBName = txtDBName.Text.Trim();
            string strTBName = txtTBName.Text.Trim();
            string strScore = txtScore.Text.Trim();
            string strBegin = txtBegin.Text.Trim();
            if (strFileName.Length == 0)
            {
                MessageBox.Show("请输入保存文件地址");
                txtFileName.Focus();
                return;
            }
            if (strAddr.Length == 0)
            {
                MessageBox.Show("请输入数据库地址");
                txtAddr.Focus();
                return;
            }
            if (strDBName.Length == 0)
            {
                MessageBox.Show("请输入数据库名称");
                txtDBName.Focus();
                return;
            }
            if (strTBName.Length == 0)
            {
                MessageBox.Show("请输入表名");
                txtTBName.Focus();
                return;
            }
            if (strScore.Length == 0)
            {
                MessageBox.Show("请输入阈值");
                txtScore.Focus();
                return;
            }
            if (strPort.Length == 0)
            {
                strPort = "3306";
            }
            if (strUserName.Length == 0)
            {
                strUserName = "root";
            }
            if (strPassword.Length == 0)
            {
                strPassword = "root";
            }

            Thread thExport = new Thread(new ParameterizedThreadStart(Export));
            thExport.Start(new string[] { strFileName, strAddr, strPort, strUserName, strPassword, strDBName, strTBName, strScore, strBegin });

        }

        void t_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            MessageBox.Show("1");
        }


        private void Export(object arg)
        {

            string[] arrArg = (string[])arg;
            string strFileName = arrArg[0];
            string strAddr = arrArg[1];
            string strPort = arrArg[2];
            string strUserName = arrArg[3];
            string strPassword = arrArg[4];
            string strDBName = arrArg[5];
            string strTBName = arrArg[6];
            string strScore = arrArg[7];
            string strBegin = arrArg[8];
            
            DBHelper dbHelper = null;
            DBHelper dbHelper2 = null;
            DataTable dtTBInfo = null;
            DataTable dtInfo = null;

            string strTBid;
            string strPersonid;
            string strResults;
            string[] arrResults;
            string[] arrResult;
            int iCount = 1;
            DataTable dtAcols = null;
            string Axpbh;
            string Apersonpk;
            string Aqyid;
            DataTable dtBcols = null;
            string Bxpbh;
            string Bpersonpk;
            string Bqyid;
            string Bscore;

            try
            {
                string strConnectionServer = string.Format(mConnection, strAddr, strDBName, strUserName, strPassword, strPort);
                dbHelper = DBHelper.GetInstance("MySql", strConnectionServer, false);
                dtTBInfo = dbHelper.GetDataTable("id,name,ip", "tableinfo");
                if (dtTBInfo == null)
                {
                    MessageBox.Show("tableinfo查询错误");
                    return;
                }
                Dictionary<string, string[]> dicTBInfo = new Dictionary<string, string[]>();
                foreach (DataRow dr in dtTBInfo.Rows)
                {
                    dicTBInfo.Add(dr["id"].ToString(), new string[] { (string)dr["name"], (string)dr["ip"] });
                }
                //////
                Dictionary<string, DBHelper> dicIP_DBHelper = new Dictionary<string, DBHelper>();
                dicIP_DBHelper.Add(strAddr, dbHelper);
                /////
                dtInfo = dbHelper.GetDataTable("id,tableid,personid,resultids,note", strTBName, "id>=" + strBegin, "id asc");
                if (dtInfo == null)
                {
                    MessageBox.Show(strTBName +"查询错误");
                    return;
                }

                Dispatcher.Invoke(new Action(delegate { pbExport.Maximum = dtInfo.Rows.Count; tbProcess.Text = string.Format("0/{0}", dtInfo.Rows.Count); }));

                
                foreach (DataRow dr in dtInfo.Rows)
                {
                    if (string.IsNullOrEmpty(dr["resultids"].ToString()))
                    {
                        Dispatcher.Invoke(new Action(delegate { pbExport.Value = iCount++; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value, dtInfo.Rows.Count);}));
                        continue;
                    }
                    else
                    {
                        strTBid = dr["tableid"].ToString();
                        strPersonid = dr["personid"].ToString();
                        strResults = (string)dr["resultids"];
                        arrResults = strResults.Trim(';').Split(';');
                        #region 获取查重的人员信息
                        dtAcols = null;
                        foreach (KeyValuePair<string, string[]> kv in dicTBInfo)
                        {
                            if (kv.Key == strTBid)
                            {
                                dbHelper2 = null;
                                foreach (KeyValuePair<string, DBHelper> kv1 in dicIP_DBHelper)
                                {
                                    if (kv1.Key == kv.Value[1])
                                    {
                                        dbHelper2 = kv1.Value;
                                        break;
                                    }
                                }
                                if (dbHelper2 == null)
                                {
                                    if (kv.Value[1] != null)
                                    {
                                        dbHelper2 = DBHelper.GetInstance("MySql", string.Format(mConnection, kv.Value[1], strDBName, strUserName, strPassword, strPort), false);
                                        dicIP_DBHelper.Add(kv.Value[1], dbHelper2);
                                    }
                                    else
                                    {
                                        dbHelper2 = dbHelper;
                                    }
                                }
                                dtAcols = dbHelper2.GetDataTable("xpbh,personpk,qyid", kv.Value[0], string.Format("id={0}", strPersonid));
                                break;
                            }                   
                        }
                        if (dtAcols ==null)
                        {
                            Dispatcher.Invoke(new Action(delegate { pbExport.Value = iCount++; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value, dtInfo.Rows.Count); }));
                            continue;
                        }
                        else
                        {
                            Axpbh = dtAcols.Rows[0]["xpbh"].ToString();
                            Apersonpk = dtAcols.Rows[0]["personpk"].ToString();
                            Aqyid = dtAcols.Rows[0]["qyid"].ToString();
                        }
                        #endregion
                        dtBcols = null;
                        arrResult = arrResults[0].Split(',');
                        if (arrResult[0] != strTBid || arrResult[1] != strPersonid)
                        {
                            if (double.Parse(arrResult[2])*100 >= double.Parse(strScore))
                            {

                                foreach (KeyValuePair<string, string[]> kv in dicTBInfo)
                                {
                                    if (kv.Key == arrResult[0])
                                    {
                                        dbHelper2 = null;
                                        foreach (KeyValuePair<string, DBHelper> kv1 in dicIP_DBHelper)
                                        {
                                            if (kv1.Key == kv.Value[1])
                                            {
                                                dbHelper2 = kv1.Value;
                                                break;
                                            }
                                        }
                                        if (dbHelper2 == null)
                                        {
                                            if (kv.Value[1] != null)
                                            {
                                                dbHelper2 = DBHelper.GetInstance("MySql", string.Format(mConnection, kv.Value[1], strDBName, strUserName, strPassword, strPort), false);
                                                dicIP_DBHelper.Add(kv.Value[1], dbHelper2);
                                            }
                                            else
                                            {
                                                dbHelper2 = dbHelper;
                                            }
                                        }
                                        dtBcols = dbHelper2.GetDataTable("xpbh,personpk,qyid", kv.Value[0], string.Format("id={0}", arrResult[1]));
                                        break;
                                    }
                                }
                                if (dtBcols != null)
                                {
                                    Bxpbh = dtBcols.Rows[0]["xpbh"].ToString();
                                    Bpersonpk = dtBcols.Rows[0]["personpk"].ToString();
                                    Bqyid = dtBcols.Rows[0]["qyid"].ToString();
                                    Bscore = arrResult[2];
                                    File.AppendAllText(strFileName, string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\r\n", Axpbh, Apersonpk, Aqyid, Bscore, Bxpbh, Bpersonpk, Bqyid), Encoding.UTF8);
                                    dbHelper.Insert("export_checkdouble", new string[] { "xpbh1", "personpk1", "qyid1", "score", "xpbh2", "personpk2", "qyid2", "note" }, new object[] { Axpbh, Apersonpk, Aqyid, Bscore, Bxpbh, Bpersonpk, Bqyid, dr["id"].ToString() });
                                }
                            }
                        }
                        for (int i = 1; i < arrResults.Length; i++)
                        {
                            arrResult = arrResults[i].Split(',');
                            if (double.Parse(arrResult[2]) * 100 >= double.Parse(strScore))
                            {
                                foreach (KeyValuePair<string, string[]> kv in dicTBInfo)
                                {
                                    if (kv.Key == arrResult[0])
                                    {
                                        dbHelper2 = null;
                                        foreach (KeyValuePair<string, DBHelper> kv1 in dicIP_DBHelper)
                                        {
                                            if (kv1.Key == kv.Value[1])
                                            {
                                                dbHelper2 = kv1.Value;
                                                break;
                                            }
                                        }
                                        if (dbHelper2 == null)
                                        {
                                            if (kv.Value[1] != null)
                                            {
                                                dbHelper2 = DBHelper.GetInstance("MySql", string.Format(mConnection, kv.Value[1], strDBName, strUserName, strPassword, strPort), false);
                                                dicIP_DBHelper.Add(kv.Value[1], dbHelper2);
                                            }
                                            else
                                            {
                                                dbHelper2 = dbHelper;
                                            }
                                        }
                                        dtBcols = dbHelper2.GetDataTable("xpbh,personpk,qyid", kv.Value[0], string.Format("id={0}", arrResult[1]));
                                        break;
                                    }
                                }
                                if (dtBcols != null)
                                {
                                    Bxpbh = dtBcols.Rows[0]["xpbh"].ToString();
                                    Bpersonpk = dtBcols.Rows[0]["personpk"].ToString();
                                    Bqyid = dtBcols.Rows[0]["qyid"].ToString();
                                    Bscore = arrResult[2];
                                    File.AppendAllText(strFileName, string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\r\n", Axpbh, Apersonpk, Aqyid, Bscore, Bxpbh, Bpersonpk, Bqyid), Encoding.UTF8);
                                    dbHelper.Insert("export_checkdouble", new string[] { "xpbh1", "personpk1", "qyid1", "score", "xpbh2", "personpk2", "qyid2", "note" }, new object[] { Axpbh, Apersonpk, Aqyid, Bscore, Bxpbh, Bpersonpk, Bqyid, dr["id"].ToString() });
                                }
                            }
                        }
                    }
                    Dispatcher.Invoke(new Action(delegate { pbExport.Value = iCount++; tbProcess.Text = string.Format("{0}/{1}", pbExport.Value, dtInfo.Rows.Count); }));
                }

                MessageBox.Show("导出完成");
            }
            catch (System.Exception ex)
            {
                MessageBox.Show(ex.ToString());
                if (dtTBInfo != null)
                {
                    dtTBInfo.Dispose();
                }
                if (dtInfo != null)
                {
                    dtInfo.Dispose();
                }
                if (dtAcols != null)
                {
                    dtAcols.Dispose();
                }
                if (dtBcols != null)
                {
                    dtBcols.Dispose();
                }      
                return;
            }
            if (dtTBInfo != null)
            {
                dtTBInfo.Dispose();
            }
            if (dtInfo != null)
            {
                dtInfo.Dispose();
            }
            if (dtAcols != null)
            {
                dtAcols.Dispose();
            }
            if (dtBcols != null)
            {
                dtBcols.Dispose();
            } 
        }




    }
}
