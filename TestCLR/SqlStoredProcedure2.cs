using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SqlStoredProcedure2()
    {
        SqlConnection sqlConnection = new SqlConnection();
        SqlCommand sqlCommand = new SqlCommand();
        try
        {
            sqlConnection = new SqlConnection("context connection=true");
            sqlConnection.Open();
            sqlCommand = new SqlCommand("INSERT INTO [dbo].[ParamTable] ([SourceFile]) VALUES('YourSourceFile3')", sqlConnection);
            SqlContext.Pipe.ExecuteAndSend(sqlCommand);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            sqlConnection.Close();
            sqlConnection.Dispose();
            sqlCommand.Dispose();
        }
    }
}
