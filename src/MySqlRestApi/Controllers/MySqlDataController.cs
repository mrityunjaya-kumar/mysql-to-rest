using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Web.Http;
using System.Web.Http.Cors;

namespace MySqlRestApi.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    public class MySqlDataController : ApiController
    {
        private static string DbConnectionString = ConfigurationManager.ConnectionStrings["DbConnectionString"].ConnectionString;
        
        [Route("db/{dbName}/tables/list")]
        [AcceptVerbs("GET")]
        public HttpResponseMessage GetTableList(string dbName)
        {
            MySqlConnection conn = new MySqlConnection(string.Format(DbConnectionString, dbName));
            List<string> tables = new List<string>();
            string query = string.Empty;
            try
            {
                MySqlCommand command = conn.CreateCommand();
                conn.Open();
                query = "SHOW TABLES";
                command.CommandText = query;

                var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        JObject row = ConvertRowToJObject(reader);
                        tables.Add(reader.GetString(0));
                    }
                }

                conn.Close();
                return Request.CreateResponse(HttpStatusCode.OK, tables);
            }
            catch (Exception ex)
            {
                JObject error = new JObject();
                error.Add("type", "Exception");
                error.Add("error", ex.Message);
#if DEBUG
                error.Add("query", query);
                error.Add("exception", JToken.FromObject(ex));
#endif
                return Request.CreateResponse(HttpStatusCode.BadRequest, error);
            }
            finally
            {
                if (conn.State != ConnectionState.Closed)
                    conn.Close();
            }
        }

        [Route("db/{dbName}/tables/{tableName}/schema")]
        [AcceptVerbs("GET")]
        public HttpResponseMessage GetTableSchema(string dbName, string tableName)
        {
            JObject obj = GetTableSchemaInJson(dbName, tableName);
            JProperty prop = obj.Property("type");
            if (prop != null && prop.Value.ToString().Equals("Exception", StringComparison.OrdinalIgnoreCase))
            {
                return Request.CreateResponse(HttpStatusCode.OK, obj);
            }
            else
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest, obj);
            }
        }

        [Route("db/{dbName}/tables/{tableName}/refs")]
        [AcceptVerbs("GET")]
        public HttpResponseMessage GetTableReferences(string dbName, string tableName)
        {
            MySqlConnection conn = new MySqlConnection(string.Format(DbConnectionString, dbName));
            List<JObject> rows = new List<JObject>();
            string query = string.Empty;
            try
            {
                MySqlCommand command = conn.CreateCommand();
                conn.Open();
                query = "SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA = '"+dbName+"' and REFERENCED_TABLE_NAME = '"+tableName+"'; ";
                command.CommandText = query;

                var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        JObject row = ConvertRowToJObject(reader);
                        rows.Add(row);
                    }
                }

                conn.Close();
                return Request.CreateResponse(HttpStatusCode.OK, rows);
            }
            catch (Exception ex)
            {
                JObject error = new JObject();
                error.Add("type", "Exception");
                error.Add("error", ex.Message);
#if DEBUG
                error.Add("query", query);
                error.Add("exception", JToken.FromObject(ex));
#endif
                return Request.CreateResponse(HttpStatusCode.BadRequest, error);
            }
            finally
            {
                if (conn.State != ConnectionState.Closed)
                    conn.Close();
            }
        }

        [Route("db/{dbName}/tables/{tableName}/rows")]
        [AcceptVerbs("GET")]
        public HttpResponseMessage GetTableRows(string dbName, string tableName)
        {
            MySqlConnection conn = new MySqlConnection(string.Format(DbConnectionString, dbName));
            List<JObject> rows = new List<JObject>();
            List<object> rows2 = new List<object>();

            string query = "";
            try
            {
                MySqlCommand command = conn.CreateCommand();
                conn.Open();

                string columns = "";
                string top = "";
                int skip = 0;
                string order = "";
                string orderColumns = "";
                string criteria = "";
                int pindex = 0;
                int psize = 10;
                string column = "";     // 
                bool distinct = false;

                try
                {
                    columns = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "columns").Value;
                }
                catch
                {
                }

                try
                {
                    column = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "column").Value;
                    if (!string.IsNullOrEmpty(column))
                    {
                        columns = column;
                        if (column.Contains(","))
                        {
                            column = "";
                        }
                    }
                }
                catch
                {
                }

                try
                {
                    var queryParam = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "distinct");
                    if (queryParam.Key.ToLower().Equals("distinct"))
                    {
                        distinct = true;
                    }
                }
                catch
                {
                }

                try
                {
                    top = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "top").Value;
                    int.TryParse(Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "skip").Value, out skip);
                }
                catch
                {
                }

                try
                {
                    order = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "order").Value;
                }
                catch
                {
                }

                try
                {
                    orderColumns = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "order-columns").Value;
                }
                catch
                {
                }

                try
                {
                    criteria = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "criteria").Value;
                }
                catch
                {
                }

                try
                {
                    int.TryParse(Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "page-index").Value, out pindex);
                    int.TryParse(Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "page-size").Value, out psize);
                }
                catch
                {
                }

                if (!string.IsNullOrEmpty(order))
                {
                    if (!order.Equals("desc", StringComparison.OrdinalIgnoreCase) && !order.Equals("desc", StringComparison.OrdinalIgnoreCase))
                    {
                        order = "";
                    }
                }
                else
                {
                    order = "";
                }

                if (!string.IsNullOrEmpty(top))
                {
                    top = " LIMIT " + skip + ", " + top;
                }
                else
                {
                    top = "";
                }

                if (psize > 0)
                {
                    top = " LIMIT " + pindex * psize + ", " + psize;
                }

                if (!string.IsNullOrEmpty(criteria))
                {
                    if (criteria.Contains(";"))
                    {
                        criteria = " WHERE " + criteria.Replace("!::", " not like ")
                                                       .Replace("::", " like ")
                                                       .Replace("!:null", " is not null ")
                                                       .Replace(":null", " is null ")
                                                       .Replace(":", "=")
                                                       .Replace(";$or.", " or ")
                                                       .Replace(";", " and ");
                    }
                    else
                    {
                        criteria = " WHERE " + criteria.Replace("::", " like ").Replace(":", "=");
                    }

                    if (!string.IsNullOrEmpty(orderColumns))
                    {
                        criteria += " ORDER BY `" + orderColumns.Replace(",", "`,`") + "` " + order;
                    }
                }
                else
                {
                    criteria = "";
                }

                if (!string.IsNullOrEmpty(columns))
                {
                    columns = "`" + columns.Replace(",", "`,`") + "`";
                    query = "SELECT " + columns + " FROM `" + tableName + "`" + criteria + top;
                }
                else
                {                    
                    query = "SELECT " + " * FROM `" + tableName + "`" + criteria + top;
                }

                command.CommandText = query;

                var result = command.ExecuteReader();
                if (result.HasRows)
                {
                    if (string.IsNullOrEmpty(column))
                    {
                        while (result.Read())
                        {
                            JObject doc = ConvertRowToJObject(result);
                            rows.Add(doc);
                        }

                        return Request.CreateResponse(HttpStatusCode.OK, rows);
                    }
                    else
                    {
                        while (result.Read())
                        {
                            object value = null;
                            try
                            {
                                value = result.IsDBNull(0) ? null : result.GetValue(0);
                            }
                            catch
                            {
                            }
                            if (value != null)
                            {
                                rows2.Add(value);
                            }
                        }
                        if (distinct)
                            rows2 = rows2.Distinct<object>().ToList<object>();
                        return Request.CreateResponse(HttpStatusCode.OK, rows2);
                    }
                }
                else
                {
                    return Request.CreateResponse(HttpStatusCode.OK, new List<string>());
                }
            }
            catch (Exception ex)
            {
                JObject error = new JObject();
                error.Add("type", "Exception");
                error.Add("error", ex.Message);
#if DEBUG
                error.Add("query", query);
                error.Add("exception", JToken.FromObject(ex));
#endif
                return Request.CreateResponse(HttpStatusCode.BadRequest, error);
            }
            finally
            {
                if (conn.State != ConnectionState.Closed)
                    conn.Close();
            }
        }

        [Route("db/{dbName}/tables/{tableName}/editor")]
        [AcceptVerbs("GET")]
        public HttpResponseMessage GetTableRowsInEditor(string dbName, string tableName)
        {
            JObject schema = GetTableSchemaInJson(dbName, tableName);
            StringBuilder sb = new StringBuilder();
            string template = File.ReadAllText(System.Web.Hosting.HostingEnvironment.MapPath("~/App_Data/templates/view-table.html"));
            sb.Append(template);
            
            string query=this.Request.RequestUri.Query;
            if (string.IsNullOrEmpty(query))
            {
                query = "";
            }
            //else
            //{
            //    query = "?" + query;
            //}

            string columns="";
            bool fc = true;
            foreach (var prop in schema.Properties())
            {
                if (!fc)
                {
                    columns += ",";
                }
                columns += "\'" + prop.Name.ToString() + "\'";
                fc = false;
            }

            sb.Replace("{dbName}", dbName);
            sb.Replace("{tableName}", tableName);
            sb.Replace("{columns}", columns);
            sb.Replace("{querystring}", query);

            var response = new HttpResponseMessage();
            response.Content = new StringContent(sb.ToString());
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("text/html");
            return response;
        }

        [Route("db/{dbName}/tables/{tableName}/rows/{id}")]
        [AcceptVerbs("GET")]
        public HttpResponseMessage GetTableRow(string dbName, string tableName, string id)
        {
            MySqlConnection conn = new MySqlConnection(string.Format(DbConnectionString, dbName));

            JObject row = new JObject();
            string query = string.Empty;
            try
            {
                
                MySqlCommand command = conn.CreateCommand();
                conn.Open();

                string columns = "";
                string key = "id";
                string keyType = "int";

                try
                {
                    columns = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "columns").Value;
                }
                catch
                {
                }

                try
                {
                    key = Request.GetQueryNameValuePairs().FirstOrDefault(q => q.Key == "key-column").Value;
                    if (!string.IsNullOrEmpty(key) && key.Contains(":"))
                    {
                        string[] keys = key.Split(':');
                        key = keys[0];
                        keyType = keys[1];
                    }
                    else
                    {
                        key = "id";
                    }
                }
                catch
                {
                }

                if (!string.IsNullOrEmpty(columns))
                {
                    columns = "`" + columns.Replace(",", "`,`") + "`";
                }
                else
                {
                    columns = "*";
                }

                id = keyType.Equals("int", StringComparison.OrdinalIgnoreCase) ? id : "'" + id + "'";
                query = "SELECT " + columns + " FROM " + tableName + " WHERE `" + key + "`=" + id;
                command.CommandText = query;

                var result = command.ExecuteReader();
                if (result.HasRows)
                {
                    result.Read();
                    row = ConvertRowToJObject(result);
                    return Request.CreateResponse(HttpStatusCode.OK, row);
                }
                
                return Request.CreateResponse(HttpStatusCode.NotFound, JValue.CreateNull());
            }
            catch (Exception ex)
            {
                JObject error = new JObject();
                error.Add("type", "Exception");
                error.Add("error", ex.Message);
#if DEBUG
                error.Add("query", query);
                error.Add("exception", JToken.FromObject(ex));
#endif
                return Request.CreateResponse(HttpStatusCode.BadRequest, error);
            }
            finally
            {
                if(conn.State!=ConnectionState.Closed)
                    conn.Close();
            }
        }

        [Route("db/{dbName}/tables/{tableName}/rows")]
        [AcceptVerbs("POST")]
        public HttpResponseMessage AddTableRow(string dbName, string tableName, JObject model)
        {
            JObject schema = GetTableSchemaInJson(dbName, tableName);
            JProperty schemaProp = schema.Property("type");
            if (schemaProp != null && schemaProp.Value.ToString().Equals("Exception", StringComparison.OrdinalIgnoreCase))
            {
                return Request.CreateResponse(HttpStatusCode.OK, schema);
            }           

            string cols = "";
            string vals = "";

            // Mandatory field check.
            foreach (var prop in schema.Properties())
            {
                var token = model.Property(prop.Name);
                JProperty nullField = ((JObject)prop.Value).Property("Null");
                JProperty defaultField = ((JObject)prop.Value).Property("Default");
                JProperty extraField = ((JObject)prop.Value).Property("Extra");
                bool isPrimary = ((JObject)prop.Value).Property("Key").Value.ToString().Equals("PRI", StringComparison.OrdinalIgnoreCase);

                if (token == null && isPrimary && extraField.Value.ToString().Equals("auto_increment", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (token == null && nullField != null &&
                    nullField.Value.ToString().Equals("no", StringComparison.OrdinalIgnoreCase) &&
                    string.IsNullOrEmpty(defaultField.Value.ToString()))
                {
                    var obj = new JObject();
                    obj.Add("state", "VALIDATION FAILURE");
                    obj.Add("error", "Column " + prop.Name + " was not found.");
                    return Request.CreateResponse(HttpStatusCode.NotFound, obj);
                }


                if (nullField != null && nullField.Value.ToString().Equals("no", StringComparison.OrdinalIgnoreCase) && token.Value == null)
                {
                    var obj = new JObject();
                    obj.Add("state", "VALIDATION FAILURE");
                    obj.Add("error", "Column " + prop.Name + " can't be null.");
                    return Request.CreateResponse(HttpStatusCode.BadRequest, schema);
                }
            }

            bool isFirstProperty = true;
            //Build insert query
            foreach (var prop in model.Properties())
            {
                var field = schema.Property(prop.Name);
                bool isPrimary = ((JObject)field.Value).Property("Key").Value.ToString().Equals("PRI", StringComparison.OrdinalIgnoreCase);
                bool isUnique = ((JObject)field.Value).Property("Key").Value.ToString().Equals("UNI", StringComparison.OrdinalIgnoreCase);
                bool isString = true;
                JToken type = ((JObject)field.Value).Property("Type").Value;
                if (type != null && type.ToString().Contains("int") || type.ToString().Contains("float") || type.ToString().Contains("double"))
                {
                    isString = false;
                }

                if (isFirstProperty)
                {
                    isFirstProperty = false;
                }
                else
                {
                    cols += ", ";
                    vals += ", ";
                }

                cols += "`" + prop.Name + "`";
                if (isString)
                    vals += "'" + prop.Value + "'";
                else
                    vals += prop.Value.ToString();

            }

            string insertQuery = "INSERT into `" + tableName + "` (" + cols + ") VALUES(" + vals + ");";

            try
            {
                MySqlConnection conn = new MySqlConnection(string.Format(DbConnectionString, dbName));
                MySqlCommand command = conn.CreateCommand();
                conn.Open();
                command.CommandText = insertQuery;

                var result = command.ExecuteNonQuery();
                conn.Close();
                if (result == 1)
                {
                    var obj = new JObject();
                    obj.Add("state", "SUCCESS");
                    obj.Add("recordsAffected", result);
                    return Request.CreateResponse(HttpStatusCode.Created, obj);
                }
                else
                {
                    var obj = new JObject();
                    obj.Add("state", "FAILED");
                    obj.Add("recordsAffected", result);
                    return Request.CreateResponse(HttpStatusCode.BadRequest, obj);
                }
            }
            catch (Exception e)
            {
                var obj = new JObject();
                obj.Add("state", "FAILURE");
                obj.Add("query", insertQuery);
                obj.Add("error", e.Message);
                return Request.CreateResponse(HttpStatusCode.BadRequest, obj);
            }
        }

        [Route("db/{dbName}/tables/{tableName}/rows/{id}")]
        [AcceptVerbs("PUT")]
        public IHttpActionResult UpdateTableRow(string dbName, string tableName, JValue id, JObject model)
        {
            throw new NotImplementedException();
        }

        [Route("db/{dbName}/tables/{tableName}/rows/{id}")]
        [AcceptVerbs("DELETE")]
        public IHttpActionResult DeleteTableRow(string dbName, string tableName, JValue id)
        {
            throw new NotImplementedException();
        }

        [Route("db/{dbName}/query")]
        [AcceptVerbs("POST")]
        public HttpResponseMessage ExecuteQuery(string dbName, [FromBody] JObject model)
        {
            JProperty queryProp = model.Property("query");
            JProperty executeProp = model.Property("execute");
            string execute = "reader";

            string query = queryProp.Value.ToString();

            if (string.IsNullOrEmpty(query))
            {
                var error = new { error = "Query is null or empty" };
                return Request.CreateResponse(HttpStatusCode.BadRequest, error);
            }

            if (executeProp != null && !string.IsNullOrEmpty(executeProp.Value.ToString()))
            {
                execute = executeProp.Value.ToString();
            }
            else
            {
                if (query.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                    execute = "reader";
                else
                    execute = "nonquery";
            }

            MySqlConnection conn = new MySqlConnection(string.Format(DbConnectionString, dbName));
            try
            {
                MySqlCommand command = new MySqlCommand(query, conn);
                //command.CommandType = type;

                JProperty paramsProp = model.Property("parameters");
                if (paramsProp != null)
                {
                    JArray paramsPropArray = paramsProp.Value as JArray;

                    foreach (var item in paramsPropArray)
                    {
                        string name = ((JObject)item).Property("name").Value.ToString();
                        MySqlDbType dbType = MySqlDbType.String;
                        Enum.TryParse<MySqlDbType>(((JObject)item).Property("dbType").Value.ToString(), out dbType);
                        int size = 0;
                        int.TryParse(((JObject)item).Property("name").Value.ToString(), out size);
                        string value = ((JObject)item).Property("value").Value.ToString();

                        MySqlParameter param = new MySqlParameter(name, dbType);
                        if (size > 0)
                            param.Size = size;
                        param.Value = value;
                        command.Parameters.Add(param);
                    }
                }

                conn.Open();
                switch (execute.ToLower())
                {
                    case "reader":
                        {
                            MySqlDataReader reader = command.ExecuteReader();
                            bool frs = true;
                            List<List<JObject>> resultSets = new List<List<JObject>>();
                            List<JObject> results;
                            int rsCount = 0;

                            do
                            {
                                if (reader.HasRows)
                                {
                                    results = new List<JObject>();
                                    while (reader.Read())
                                    {
                                        JObject result = ConvertRowToJObject(reader);
                                        results.Add(result);
                                    }

                                    resultSets.Add(results);
                                }
                                else
                                {
                                    results = new List<JObject>();
                                    resultSets.Add(new List<JObject>());
                                }

                                rsCount++;
                            }
                            while (reader.NextResult());

                            if (!reader.IsClosed) reader.Close();

                            if (rsCount == 1)
                            {

                                return Request.CreateResponse(HttpStatusCode.OK, results);
                            }
                            else
                            {
                                return Request.CreateResponse(HttpStatusCode.OK, resultSets);
                            }
                           
                        }
                    case "nonquery":
                        {
                            int count = command.ExecuteNonQuery();
                            var result = new { recordsAffected = count };
                            return Request.CreateResponse(HttpStatusCode.OK, result);
                        }
                    case "scalar":
                        {
                            var result = command.ExecuteScalar();
                            return Request.CreateResponse(HttpStatusCode.OK, result);
                        }
                }

                return Request.CreateResponse(HttpStatusCode.OK, new Object());
            }
            catch (Exception ex)
            {
                JObject error = new JObject();
                error.Add("type", ex.GetType().Name);
                error.Add("error", ex.Message);
#if DEBUG
                error.Add("query", queryProp.Value.ToString());
                error.Add("exception", JToken.FromObject(ex));
#endif
                return Request.CreateResponse(HttpStatusCode.BadRequest, error);
            }
            finally {
                conn.Close();
            }
        }

        [Route("db/{dbName}/sprocs/{procName}")]
        [AcceptVerbs("POST")]
        public IHttpActionResult ExecuteStoredProcedures(string dbName, string tableName, string procName)
        {
            throw new NotImplementedException();
        }

        [Route("db/{dbName}/views/{viewName}")]
        [AcceptVerbs("POST")]
        public IHttpActionResult ExecuteView(string dbName, string tableName, string viewName)
        {
            throw new NotImplementedException();
        }

        private static JObject ConvertRowToJObject(MySqlDataReader result)
        {
            JObject row = new JObject();
            try
            {
                for (int i = 0; i < result.FieldCount; i++)
                {
                    string key = result.GetName(i).Replace(".", ":");
                    object value = null;
                    try
                    {
                       value = result.IsDBNull(i) ? null : result.GetValue(i);
                    }
                    catch
                    {
                    }

                    if (value == null)
                        value = JValue.CreateNull();
                    JToken token = JToken.FromObject(value);
                    row.Add(key, token);
                }

                return row;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static JObject GetTableSchemaInJson(string dbName, string tableName)
        {
            MySqlConnection conn = new MySqlConnection(string.Format(DbConnectionString, dbName));
            JObject schema = new JObject();
            string query = string.Empty;
            try
            {
                MySqlCommand command = conn.CreateCommand();
                conn.Open();
                query = "SHOW COLUMNS FROM `" + tableName + "`";
                command.CommandText = query;

                var result = command.ExecuteReader();
                if (result.HasRows)
                {
                    while (result.Read())
                    {
                        JObject row = ConvertRowToJObject(result);
                        JProperty prop = row.Property("Field");
                        schema.Add(prop.Value.ToString(), row);
                        row.Remove("Field");
                    }
                }

                return schema;
            }
            catch (Exception ex)
            {
                JObject error = new JObject();
                error.Add("type", "Exception");
                error.Add("error", ex.Message);
#if DEBUG
                error.Add("query", query);
                error.Add("exception", JToken.FromObject(ex));
#endif
                return error;
            }
            finally
            {
                if (conn.State != ConnectionState.Closed)
                    conn.Close();
            }
        }
    }
}
