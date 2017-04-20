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
    public partial class MainWindow : Window
    {

        private string mConnection = "Data Source={0};Database={1};User Id={2};Password={3};pooling=true;CharSet=utf8;port={4};";

        public MainWindow()
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

            if (strPort.Length == 0)
            {
                strPort = "3306";
            }
            if(strUserName.Length==0)
            {
                strUserName = "root";
            }
            if (strPassword.Length == 0)
            {
                strPassword = "root";
            }

            Thread thExport = new Thread(new ParameterizedThreadStart(Export));
            thExport.Start(new string[] { strFileName,strAddr,strPort,strUserName,strPassword });
            
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

          
            string strConnectionServer = string.Format(mConnection, strAddr, "faceserverdb", strUserName, strPassword, strPort);
            DBHelper dbHelperServer = DBHelper.GetInstance("MySql", strConnectionServer, false);
            DataTable dtTBInfo = dbHelperServer.GetDataTable("id,name", "tableinfo");
            if (dtTBInfo == null)
            {
                MessageBox.Show("数据库执行错误");
                return;
            }
            Dictionary<string, string> dicTBInfo = new Dictionary<string, string>();
            foreach (DataRow dr in dtTBInfo.Rows)
            {
                dicTBInfo.Add(dr["id"].ToString(), (string)dr["name"]);
            }

            string strConnection = string.Format(mConnection, strAddr, "facecomparisondb", strUserName, strPassword, strPort);
            DBHelper dbHelper = DBHelper.GetInstance("MySql", strConnection, false);

            DataTable dtInfo = dbHelper.GetDataTable("comparisonid,resultids", "comparisoninfo");
            if (dtInfo == null)
            {
                MessageBox.Show("数据库执行错误");
                return;
            }

            Dispatcher.Invoke(new Action(delegate { pbExport.Maximum = dtInfo.Rows.Count; }));

           

            string strResults;
            string[] arrResults;
            string[] arrResult;
            int iCount=1;
            StringBuilder sbResults = new StringBuilder();
            foreach (DataRow dr in dtInfo.Rows)
            {
                strResults = (string)dr["resultids"];
                arrResults = strResults.Trim(';').Split(';');
                foreach (string strResult in arrResults)
                {
                    arrResult = strResult.Split(',');
                    sbResults.AppendFormat("{0}:{1};", GetUserName(arrResult[1], dicTBInfo[arrResult[0]], dbHelperServer), Convert.ToDouble(arrResult[2]) * 100);
                }

                File.AppendAllText(strFileName, string.Format("{0},{1}\r\n", dr["comparisonid"], sbResults.ToString()), Encoding.UTF8);
                sbResults.Clear();

                Dispatcher.Invoke(new Action(delegate { pbExport.Value = iCount++; }));
            }

            MessageBox.Show("导出完成");
        }

        private string GetUserName(string ID, string TbName, DBHelper dbHelper)
        {
            DataTable dtName = dbHelper.GetDataTable("xm", TbName,string.Format("id={0}",ID));
            if (dtName == null)
            {
                return "";
            }

            return (string)dtName.Rows[0]["xm"];
        }

    }
}
