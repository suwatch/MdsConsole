using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Cis.Monitoring.DataAccess;
using Microsoft.Cis.Monitoring.Mds.mdscommon;
using Newtonsoft.Json.Linq;

namespace MdsConsole
{
    class Program
    {
        static int _suffix = new Random().Next(100, 1000);
        static SemaphoreSlim _semaphore = new SemaphoreSlim(initialCount: 4);
        static Uri _mdsUri = new Uri("https://production.diagnostics.monitoring.core.windows.net/");
        static MdsDataAccessClient _client;
        static string[] _tableNames = new string[] 
            {
                "WAWSAntaresIISLogFrontEndTablePRODBAY013Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODBLU013Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODCH1003Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODHK1003Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODDB3007Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODAM2011Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODKW1001Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODOS1001Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODSG1001Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODCQ1001Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODML1001Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODSY3001Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODDM1001Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODBN1003Ver6v0",
                "WAWSAntaresIISLogFrontEndTablePRODSN1001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODBAY013Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODBLU013Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODCH1003Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODHK1003Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODDB3007Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODAM2011Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODKW1001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODOS1001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODSG1001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODCQ1001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODML1001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODSY3001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODDM1001Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODBN1003Ver6v0",
                "WAWSAntaresIISLogWorkerTablePRODSN1001Ver6v0",
            };
        static string _query = "Sc_status = 400 AND S_reason = \"RequestLength\"";
        static int _pendings = 0;
        static ManualResetEvent _completed = new ManualResetEvent(false);
        static DateTime _to = new DateTime(2015, 4, 16, 00, 00, 00, DateTimeKind.Utc);
        static DateTime _from = _to.Subtract(TimeSpan.FromDays(1));
        //static DateTime _to = new DateTime(2015, 4, 13, 23, 45, 00, DateTimeKind.Utc);
        //static DateTime _from = _to.Subtract(TimeSpan.FromMinutes(5));
        static TimeSpan _interval = TimeSpan.FromMinutes(15);
        static string[] _columnNames = new string[] 
            {
                "PreciseTimeStamp",
                "S_sitename",
                "Cs_host",
                "Cs_bytes",
                "Cs_uri_stem",
                "S_reason",
            };

        static object _thisLock = new object();

        public class Arguments
        {
            public string MdsUri { get; set; }
            public string[] TableNames { get; set; }
            public string Query { get; set; }
            public string[] ColumnNames { get; set; }
            public DateTime? To { get; set; }
            public DateTime? From { get; set; }
            public string Interval { get; set; }
        }

        static void Main(string[] args)
        {
            DateTime start = DateTime.UtcNow;
            try
            {
                var arg = args.Length > 0 ? JObject.Parse(File.ReadAllText(args[0])).ToObject<Arguments>() : null;
                if (arg == null)
                {
                    arg = new JObject().ToObject<Arguments>();
                }

                _mdsUri = new Uri(String.IsNullOrEmpty(arg.MdsUri) ? "https://production.diagnostics.monitoring.core.windows.net/" : arg.MdsUri);
                _tableNames = arg.TableNames ?? _tableNames;
                _query = arg.Query;
                _columnNames = arg.ColumnNames ?? new string[0];

                Console.WriteLine("MdsUri:      {0}", _mdsUri);
                Console.WriteLine("TableNames:  {0}", String.Join(",", _tableNames));
                Console.WriteLine("Query:       {0}", _query);
                Console.WriteLine("ColumnNames: {0}", String.Join(",", _columnNames));

                if (arg.From == null && arg.To == null)
                {
                    _to = DateTime.UtcNow;
                    _from = _to.Subtract(TimeSpan.FromHours(1));
                }
                else if (arg.From == null)
                {
                    _to = arg.To.Value;
                    _from = _to.Subtract(TimeSpan.FromHours(1));
                }
                else if (arg.To == null)
                {
                    _from = arg.From.Value;
                    _to = _from.Add(TimeSpan.FromHours(1));
                }
                else
                {
                    _from = arg.From.Value;
                    _to = arg.To.Value;
                }

                Console.WriteLine("From:        {0}Z", _from.ToString("s"));
                Console.WriteLine("To:          {0}Z", _to.ToString("s"));

                _interval = String.IsNullOrEmpty(arg.Interval) ? TimeSpan.FromMinutes(15) : TimeSpan.Parse(arg.Interval);
                Console.WriteLine("Interval:    {0}", _interval);

                Run();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex); 
            }

            Console.WriteLine("Start at {0}Z, Ellapsed = {1}", start.ToString("s"), DateTime.UtcNow - start);
        }

        static void Run()
        {
            _client = new MdsDataAccessClient(_mdsUri);

            var tableNames = new List<string>();
            foreach (var tableName in _tableNames)
            {
                if (tableName.Contains("*"))
                {
                    tableNames.AddRange(_client.GetTables(tableName.Replace(".*", "*").Replace("*", ".*").Replace(".?", "?").Replace("?", ".?")));
                }
                else
                {
                    tableNames.Add(tableName);
                }
            }

            for (var current = _from; current < _to; current += _interval)
            {
                foreach (var tableName in tableNames)
                {
                    Interlocked.Increment(ref _pendings); 
                    
                    var state = new MdsState 
                    { 
                        TableName = tableName, 
                        From = current, 
                        To = current + _interval,
                        StartTime = DateTime.UtcNow
                    };
                    _semaphore.Wait();
                    try
                    {
                        var ar = _client.BeginGetTabularData(
                            state.TableName,
                            state.From,
                            state.To,
                            _query,
                            false,
                            null,
                            OnGetTabularData,
                            state);
                        if (ar.CompletedSynchronously)
                        {
                            HandleResult(ar);
                        }
                    }
                    catch (Exception ex)
                    {
                        DumpError(state, ex);
                        Release();
                    }
                }
            }

            _completed.WaitOne();
        }

        static void OnGetTabularData(IAsyncResult ar)
        {
            if (ar.CompletedSynchronously)
            {
                return;
            }

            HandleResult(ar);
        }

        static void HandleResult(IAsyncResult ar)
        {
            var state = (MdsState)ar.AsyncState;
            try
            {
                DumpRecords(state, _client.EndGetTabularData(ar));
            }
            catch (Exception ex)
            {
                DumpError(state, ex);
            }
            finally
            {
                Release();
            }
        }

        static void Release()
        {
            _semaphore.Release();
            if (0 == Interlocked.Decrement(ref _pendings))
            {
                _completed.Set();
            }
        }

        static void DumpError(MdsState state, Exception ex)
        {
            var strb = new StringBuilder();
            strb.AppendLine("{0}, {1}Z, {2}Z, {3}secs, {4}", state.TableName, state.From.ToString("s"), state.To.ToString("s"), state.EllapsedSecs, ex);
            strb.AppendLine();
            WriteLine(state, strb);
        }

        static void DumpRecords(MdsState state, IEnumerable<GenericLogicEntity> records)
        {
            var strb = new StringBuilder();
            strb.AppendLine("{0}, {1}Z, {2}Z, {3}secs, {4} records", state.TableName, state.From.ToString("s"), state.To.ToString("s"), state.EllapsedSecs, records.Count());
            foreach (var record in records.Select(r => r.ToDictionary()))
            {
                strb.Append(state.TableName);
                if (_columnNames.Length == 0)
                {
                    foreach (var pair in record)
                    {
                        strb.AppendFormat(", {0}", pair.Value);
                    }
                }
                else
                {
                    foreach (var key in _columnNames)
                    {
                        strb.AppendFormat(", {0}", FieldToString(record[key]));
                    }
                }
                strb.AppendLine();
            }
            strb.AppendLine();
            WriteLine(state, strb);
        }

        static string FieldToString(object obj)
        {
            if (obj == null)
            {
                return null;
            }

            if (obj is DateTime)
            {
                return ((DateTime)obj).ToString("s") + 'Z';
            }

            if (obj is DateTimeOffset)
            {
                return ((DateTimeOffset)obj).ToString("s") + 'Z';
            }

            return obj.ToString(); 
        }

        static void WriteLine(MdsState state, StringBuilder strb)
        {
            lock (_thisLock)
            {
                File.AppendAllLines(String.Format(@"c:\temp\MDS_{0}_{1}.log", state.TableName, _suffix), new[] { strb.ToString() });
                Console.WriteLine(strb.ToString());
            }
        }

        class MdsState
        {
            public string TableName { get; set; }
            public DateTime From { get; set; }
            public DateTime To { get; set; }
            public DateTime StartTime { get; set; }
            public int EllapsedSecs { get { return (int)DateTime.UtcNow.Subtract(StartTime).TotalSeconds; } }
        }
    }
}
