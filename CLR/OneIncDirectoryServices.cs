using System;
using System.Data;
using System.Globalization;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.IO;
using System.DirectoryServices.AccountManagement;


public partial class OneIncDirectoryServices
{
    [SqlProcedure]
    public static void UserExistsInAD(SqlString Users, SqlString Domain)
    {
        string[] UserArr = Users.Value.Split(',');

        string ADdomain = Domain.IsNull ? ProcessOneDWDomainName : Domain.Value;

        using (PrincipalContext domainContext = new PrincipalContext(ContextType.Domain, ADdomain))
        {

            // Create the record and specify the metadata for the columns.
            SqlDataRecord record = new SqlDataRecord(
                new SqlMetaData("UserName", SqlDbType.NVarChar, 50),
                new SqlMetaData("DoesExist", SqlDbType.Bit));

            // Mark the begining of the result-set.
            SqlContext.Pipe.SendResultsStart(record);

            foreach (string UserName in UserArr)
            {
                UserPrincipal DoesExist = UserPrincipal.FindByIdentity(domainContext, IdentityType.SamAccountName, UserName);
                // Set values for each column in the row.
                record.SetString(0, UserName);
                record.SetBoolean(1, DoesExist !=null ? true : false);

                // Send the row back to the client.
                SqlContext.Pipe.SendResultsRow(record);
            }

            // Mark the end of the result-set.
            SqlContext.Pipe.SendResultsEnd();
        }
    }

    public static string ProcessOneDWDomainName
    {
        get
        {
            return new SqlString(GetOption("ProcessOneDWDomainName")).Value;
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

