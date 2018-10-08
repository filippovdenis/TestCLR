using System;
using System.Data;
using System.Globalization;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;


public partial class UserDefinedFunctions
{

    public static void GenerateHourIDs_FillRow( object HourIDObj,
                                                    out SqlInt64 HourID)
    {
        HourID = new SqlInt64((long)HourIDObj);
    }

    [SqlFunction(DataAccess = DataAccessKind.Read,FillRowMethodName = "GenerateHourIDs_FillRow", TableDefinition = "HourID bigint")]
    public static IEnumerable GenerateHourIDs([SqlFacet(IsNullable = false)] SqlDateTime startDate
                                                , [SqlFacet(IsNullable = false)] SqlDateTime endDate)
    {
        ArrayList resultCollection = new ArrayList();

        DateTime startDateTime = startDate.Value;
        DateTime endDateTime = endDate.Value;

        while(startDateTime< endDateTime)
        {
            resultCollection.Add(long.Parse(startDateTime.ToString("yyyyMMddHH")));

            startDateTime = startDateTime.AddHours(1);
        }
                    
        return resultCollection;
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlDateTime GetDateByDayID(SqlInt32 DateID)
    {
        if (DateID.IsNull || DateID.Value <= 0)
            return SqlDateTime.Null;

        return DateID.IsNull ? new SqlDateTime() : new SqlDateTime(DateByDayID(DateID.Value));
    }

    protected static DateTime DateByDayID(int DateID)
    {
        string s = DateID.ToString();
        int year = int.Parse(s.Substring(0, 4));
        int month = int.Parse(s.Substring(4, 2));
        int day = int.Parse(s.Substring(6, 2));

        return new DateTime(year,
            month,
            day);
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlString GetWeekDayTypeByDayID(SqlInt32 DateID)
    {
        return DateID.IsNull ? new SqlString() : new SqlString(WeekDayTypeByDayID(DateID.Value));
    }

    protected static string WeekDayTypeByDayID(int DateID)
    {
         byte dayNumber = (byte)GetDateByDayID(DateID).Value.DayOfWeek;

        if(dayNumber == 0 || dayNumber == 6)
            return "Weekend";

        return "Weekday";
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlByte GetDayByDayID(SqlInt32 DateID)
    {
        return new SqlByte((byte)GetDateByDayID(DateID).Value.Day);
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlString GetDayOfWeekByDayID(SqlInt32 DateID)
    {
        return DateID.IsNull ? new SqlString() : new SqlString(DayOfWeekByDayID(DateID.Value));
    }

    public static string DayOfWeekByDayID(int DateID)
    {
        return DateByDayID(DateID).DayOfWeek.ToString();
    }


    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlString GetDayOfWeekByDayNumber(SqlByte DayNumber)
    {
        return new SqlString(((System.DayOfWeek)(DayNumber.Value-1)).ToString());
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlByte GetDayNumberOfWeekByDayID(SqlInt32 DateID)
    {
        byte dayNumber = (byte)GetDateByDayID(DateID).Value.DayOfWeek;
        dayNumber++;
        return new SqlByte(dayNumber);
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlByte GetWeekNumberOfYearByDayID(SqlInt32 DateID)
    {
          return new SqlByte((byte)CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(GetDateByDayID(DateID).Value
                                                                                    ,CalendarWeekRule.FirstDay
                                                                                    ,DayOfWeek.Sunday));
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlDateTime GetDateByMonthID(SqlInt32 MonthID)
    {
        if (MonthID.IsNull || MonthID.Value <= 0)
            return SqlDateTime.Null;

        string s = MonthID.ToString();
        int year = int.Parse(s.Substring(0, 4));
        int month = int.Parse(s.Substring(4, 2));

        return new SqlDateTime(year,
            month,
            1);
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlDateTime GetDateByYearID(SqlInt32 YearID)
    {
        if (YearID.IsNull || YearID.Value <= 0)
            return SqlDateTime.Null;

        string s = YearID.ToString();
        int year = int.Parse(s.Substring(0, 4));

        return new SqlDateTime(year,
            1,
            1);
    }


    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlString GetMonthNameByMonthID(SqlInt32 MonthID)
    {
        return new SqlString(GetDateByMonthID(MonthID).Value.ToString("MMMM"));
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlByte GetMonthByMonthID(SqlInt32 MonthID)
    {
        return new SqlByte((byte)GetDateByMonthID(MonthID).Value.Month);
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true,IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlByte GetQuarterByMonthID(SqlInt32 MonthID)
    {
        return new SqlByte((byte)((GetDateByMonthID(MonthID).Value.Month - 1) / 3 + 1));
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlDateTime GetDateByHourID(SqlInt64 HourID)
    {
        if (HourID.IsNull || HourID.Value <= 0)
            return SqlDateTime.Null;

        return new SqlDateTime(DateByHourID(HourID.Value));
    }

    protected static DateTime DateByHourID(long HourID)
    {
        string s = HourID.ToString();
        int year = int.Parse(s.Substring(0, 4));
        int month = int.Parse(s.Substring(4, 2));
        int day = int.Parse(s.Substring(6, 2));
        int hour = int.Parse(s.Substring(8, 2));

        return new DateTime(year,
            month,
            day,
            hour,
            1,
            1);
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlByte GetHourByHourID(SqlInt64 HourID)
    {
        return new SqlByte((byte)GetDateByHourID(HourID).Value.Hour);
    }
    /*
    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlByte GetTimeBand(SqlInt64 HourID)
    {
        return HourID.IsNull ? new SqlByte() : new SqlByte(TimeBand(HourID.Value));
    }

    public static byte TimeBand(long HourID)
    {
        DateTime datetime = DateByHourID(HourID);
        string weekDayName = datetime.DayOfWeek.ToString();
        int hour = datetime.Hour;

        
        if (weekDayName == "Saturday" ||
            weekDayName == "Sunday")
            return 0;

        if (hour >= 8 && hour <= 19)
            return 1;

        return 0;

        throw new ApplicationException("Unknown Time Band used in GetTimeBand function.");
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlInt64 GetNullDateDimHourID()
    {
        return SqlInt64.Parse((DateTime.Now.Year + 1).ToString() + "123123");
    }

    [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
    public static SqlInt32 GetEasterDayID(SqlInt32 year)
    {
        if (year.IsNull)
            return new SqlInt32();
   
        DateTime EasterDate = UserDefinedFunctions.EasterDate(year.Value);
        EasterDate = EasterDate.Subtract(new TimeSpan(2, 0, 0, 0));

        return new SqlInt32(int.Parse(String.Format("{0,4}{1:00}{2:00}", EasterDate.Year, EasterDate.Month, EasterDate.Day)));
    }

    protected static DateTime EasterDate(int Year)
    {
        // Gauss Calculation
        ////////////////////

        int Month = 3;

        // Determine the Golden number:
        int G = Year % 19 + 1;

        // Determine the century number:
        int C = Year / 100 + 1;

        // Correct for the years who are not leap years:
        int X = (3 * C) / 4 - 12;

        // Mooncorrection:
        int Y = (8 * C + 5) / 25 - 5;

        // Find sunday:
        int Z = (5 * Year) / 4 - X - 10;

        // Determine epact(age of moon on 1 januari of that year(follows a cycle of 19 years):
        int E = (11 * G + 20 + Y - X) % 30;
        if (E == 24) { E++; }
        if ((E == 25) && (G > 11)) { E++; }

        // Get the full moon:
        int N = 44 - E;
        if (N < 21) { N = N + 30; }

        // Up to sunday:
        int P = (N + 7) - ((Z + N) % 7);

        // Easterdate: 
        if (P > 31)
        {
            P = P - 31;
            Month = 4;
        }
        return new DateTime(Year, Month, P);
    }
    */
};

