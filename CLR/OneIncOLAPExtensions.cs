using System;
using System.Xml;
using System.Data;
using System.Globalization;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using Microsoft.SqlServer.Server;
using Microsoft.AnalysisServices.Xmla;

public partial class OneIncOLAPExtensions
{

    /*
    [SqlProcedure]
    public static void SendXMLA(SqlString XMLA)
    {
        XmlaClient clnt = new XmlaClient();
        clnt.Connect(SSAS_Server.Value);
        string result = clnt.Send(XMLA.Value, null);
        clnt.Disconnect();

        SqlDataRecord record = new SqlDataRecord(new SqlMetaData("DiscoverXMLA", SqlDbType.NVarChar, -1));
        // Populate the record.  
        record.SetSqlString(0, result);
        // Send the record to the client.  
        SqlContext.Pipe.Send(record);
    }
    */

    [SqlProcedure]
    public static void SendXMLA([SqlFacet(MaxSize = -1, IsNullable = false)] SqlString XMLA
                                , out SqlXml ReturnXMLA
                                , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_Server)
    {
        XmlaClient clnt = new XmlaClient();
        clnt.Connect(SSAS_Server.IsNull ? OneIncOLAPExtensions.SSAS_Server.Value : SSAS_Server.Value);

        using (var reader = new StringReader(clnt.Send(XMLA.Value, null)))
            using (var xmlreader = new XmlTextReader(reader))
            ReturnXMLA = new SqlXml(xmlreader);

        clnt.Disconnect();
    }

    [SqlProcedure]
    public static void ExecuteOLAP([SqlFacet(MaxSize = -1, IsNullable = false)] SqlString MDX
                                     , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_Server
                                     , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_DB)
    {
        OLAPExtensions.StoredProcedures.ExecuteOLAP(SSAS_Server.IsNull ? OneIncOLAPExtensions.SSAS_Server.Value : SSAS_Server.Value
                                                    , SSAS_DB.IsNull ? OneIncOLAPExtensions.SSAS_DB.Value : SSAS_DB.Value
                                                    , MDX.Value);
    }

    [SqlProcedure]
    public static void ExecuteOLAPDMV([SqlFacet(MaxSize = -1, IsNullable = false)] SqlString MDX
                                        , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_Server
                                        , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_DB)
    {
        OLAPExtensions.StoredProcedures.ExecuteOLAPDMV(SSAS_Server.IsNull ? OneIncOLAPExtensions.SSAS_Server.Value : SSAS_Server.Value
                                                        , SSAS_DB.IsNull ? OneIncOLAPExtensions.SSAS_DB.Value : SSAS_DB.Value
                                                        , MDX.Value);
    }

    [SqlProcedure]
    public static void ExecuteOLAP_Impersonate([SqlFacet(MaxSize = -1, IsNullable = false)] SqlString MDX
                                                , [SqlFacet(MaxSize = 255, IsNullable = false)] SqlString NTDomainName
                                                , [SqlFacet(MaxSize = 255, IsNullable = false)] SqlString NTUserName
                                                , [SqlFacet(MaxSize = 255, IsNullable = false)] SqlString NTPassword
                                                , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_Server
                                                , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_DB)
    {
        OLAPExtensions.StoredProcedures.ExecuteOLAP_Impersonate(SSAS_Server.IsNull ? OneIncOLAPExtensions.SSAS_Server.Value : SSAS_Server.Value
                                                                   , SSAS_DB.IsNull ? OneIncOLAPExtensions.SSAS_DB.Value : SSAS_DB.Value
                                                                   , MDX.Value
                                                                   , NTDomainName.Value
                                                                   , NTUserName.Value
                                                                   , NTPassword.Value);
    }

    [SqlProcedure]
    public static void ExecuteXMLA([SqlFacet(MaxSize = -1, IsNullable = false)] string XMLA
                                    , [SqlFacet(MaxSize = 255, IsNullable = true)] SqlString SSAS_Server)
    {
        //OLAPExtensions.StoredProcedures.ExecuteXMLA(SSAS_Server, SSAS_DB, XMLA);

        SqlXml ReturnXMLA;

        SendXMLA(XMLA
                    , out ReturnXMLA
                    , SSAS_Server);

        CheckReturnXMLAForErrors(ReturnXMLA);

    }

    public static void CheckReturnXMLAForErrors(SqlXml ReturnXMLA)
    {
        if (ReturnXMLA.IsNull)
            return;

        Exception Exception = null;

        using (SqlConnection conn = new SqlConnection("context connection=true"))   //Create current context connection
        {
            using (SqlCommand cmd = new SqlCommand(@"SELECT  d.p.value('(@ErrorCode)[1]', 'BIGINT') AS ErrorCode
		                                                        ,d.p.value('(@Description)[1]', 'NVARCHAR(MAX)') AS ErrorDescription
		                                                        ,d.p.value('(@Source)[1]', 'NVARCHAR(250)') AS [Source]
	                                                    FROM @XMLA.nodes('/*:return/*:results/*:root/*:Messages/*:Error') d(p)"
                                                    ,conn))
            {
                SqlParameter param = new SqlParameter("@XMLA", SqlDbType.Xml);
                param.Value = ReturnXMLA;
                cmd.Parameters.Add(param);

                cmd.Connection.Open();  //Open the context connetion



                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    DataTable errors = new DataTable();
                    errors.Load(reader);

                    int RowCount = errors.Rows.Count;

                    if (RowCount > 0)
                        for (int i = RowCount - 1; i >= 0; i--)
                        {
                            DataRow row = errors.Rows[i];
                            string ErrorCode = row["ErrorCode"].ToString(); // get Error Code
                            string ErrorDescription = row["ErrorDescription"].ToString(); // get Error Description
                            string Source = row["Source"].ToString(); // get Error Source

                            string message = string.Format("Error Code = {0}, Description = {1}, Source = {2}"
                                                            , ErrorCode
                                                            , ErrorDescription
                                                            , Source);
                            if (Exception == null)
                                Exception = new Exception(message);
                            else
                                Exception = new Exception(message, Exception);
                        }
                }
            }
            conn.Close();
        }

        if (Exception != null)
            throw Exception;

    }

    public static SqlString SSAS_Server
    {
        get
        {
            return new SqlString(GetOption("SSAS_Server"));
        }
    }

    public static SqlString SSAS_DB
    {
        get
        {
            return new SqlString(GetOption("SSAS_DB"));
        }
    }

    public static string GetOption(string OptionName)
    {
        string result = "";

        using (SqlConnection conn = new SqlConnection("context connection=true"))   //Create current context connection
        {
            conn.Open();  //Open the context connetion

            using (SqlCommand cmd = new SqlCommand("SELECT FlagValue FROM dbo.Options WHERE FlagName = @FlagName", conn))
            {
                SqlParameter param = new SqlParameter("@FlagName", SqlDbType.VarChar);
                param.Value = OptionName;
                cmd.Parameters.Add(param);

                result = (string)cmd.ExecuteScalar();
            }
        }

        return result;
    }
};

