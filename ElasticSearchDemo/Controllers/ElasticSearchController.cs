using ElasticSearchDemo.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Nest;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSearchDemo.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class ElasticSearchController : ControllerBase
    {
        private readonly ElasticClient elasticClient;

        public ElasticSearchController(ElasticClient elasticClient)
        {
            this.elasticClient = elasticClient;
        }
        /// <summary>
        /// Gets all the data from specific index(users).
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public async Task<IEnumerable<UserDTO>> GetAll()
        {
            var response = await elasticClient.SearchAsync<UserDTO>(s => s.
            Index("users"));
            return response.Documents;
        }
        /// <summary>
        /// Gets the data that matches with the associated id(name).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        [HttpGet("{id}")]
        public async Task<UserDTO> Get(string id)
        {
            var response = await elasticClient.SearchAsync<UserDTO>(s => s
            .Index("users")
            .Query(q => q
            .Term(t => t.Name, id)
            ||
            q.Match(m => m.Field(f => f.Name).Query(id))));


            return response?.Documents?.FirstOrDefault();
        }

        /// <summary>
        /// Gets the data that matches with the associated id(without specifying index).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        [HttpGet("{id}")]
        public async Task<Dictionary<string, object>> GetWithoutIndex(string id)
        {
            //Task<Dictionary<string, object>>
               // var response = await elasticClient.SearchAsync<dynamic>(s => s
               // .AllIndices()
               // .Query(q => q
               // .Term("name", id)
                
               //));

            var response = await elasticClient.SearchAsync<dynamic>(s => s.
            AllIndices()
           
            .Query(q => q
            .MultiMatch(m =>
            m.Fields(f => f.Field("name")
            .Field("title"))
            .Query(id))));
            //var response = await elasticClient.SearchAsync<UserDTO>(s => s.
            //AllIndices().
            //Query(q => q.
            //Match(m => m.
            //Field(f=>f.Name)
            //.Query(id))
            //&&
            //q.
            //Match(m => m.
            //Field(f=>f.Age)
            //.Query("22")
            //)));
            return (Dictionary<string,object>)response.Documents;
           // return (JObject)(response?.Documents);
        }
        [HttpGet]
        public async Task<IEnumerable<Dynamic>> GetUsingWildcard(string query)
        {
            var response = await elasticClient.SearchAsync<Dynamic>(s => s
        .From(0)
        .Take(10)
        .AllIndices()

        .Query(q => q
            .Bool(b => b
                .Should(m => m
                    .Wildcard(w => w
                    
                     .Field("name")
                    
                       
                            
                            .Value(query+"*")
                     )
                 )
             )
         ));
            //var response = await elasticClient.SearchAsync<dynamic>(s => s
            //                                  .AllIndices()
            //                                  .Query(qry => qry
            //                                             .Bool(b => b
            //                                                       .Must(m => m
            //                                                                 .QueryString(qs => qs
            //                                                                                  .DefaultField("_all")
            //                                                                                  .Query(query)))))
            //                                  .Highlight(h =>
            //                                                 h.Fields(f => f.Field("*"))));
            //var response = await elasticClient.SearchAsync<dynamic>(s => s.
            //AllIndices().
            //From(0).
            //Take(10).
            //Query(q => q.
            //Bool(b => b.
            //Must(m => m.
            //QueryString(qs => qs.
            //DefaultField("_all").
            //Query(query))))));

            return response.Documents;

        }
        [HttpGet]
        public async Task<IEnumerable<UserDTO>> GetWithSorting()
        {
            var response = await elasticClient.SearchAsync<UserDTO>(s => s.Index("users").MatchAll().Sort(s => s.Ascending(a=>a.Name)));
            return response?.Documents;
        }
        /// <summary>
        /// Getting all the data with specific fields(using "source").
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public async Task<IEnumerable<UserDTO>> GetAllWithSpecificFields()
        {
            var response = await elasticClient.SearchAsync<UserDTO>(s => s
            .Index("users")
            //can also use stored fields instead of source to get specific fields!
            .Source(so => so
            .Includes(i => i
            .Fields(new string[] {"name","age"})
            //.Fields(f => f.Name, f => f.Age)
            )
            ).Query(q => q.MatchAll()));
            return response?.Documents;
        }
        [HttpGet]
        public async Task<IEnumerable<UserDTO>> GetUsingFuzziness(string query)
        {
            var response = await elasticClient.SearchAsync<UserDTO>(s => s
            .Index("users")
            .Query(q => q
            .MultiMatch(mu => mu
            .Fields(f => f
            .Fields(new string[] { "name", "education" })
            ).Fuzziness(Fuzziness.Auto).Query(query))));

            return response?.Documents;
        }
        /// <summary>
        /// Get using boolean OR operator fields.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<IEnumerable<Dynamic>> GetUsingBoolOperators(string query)
        {
            var response = await elasticClient.SearchAsync<Dynamic>(s => s
            .AllIndices()
            .Query(q => q
            .Match(m => m
            .Field("name").Query(query))
            ||
            q.Match(m => m
            .Field("title").Query(query))
            )
            )
            ;
            return response?.Documents;
        }
        /// <summary>
        /// Gets the data searching from multiple fields.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<IEnumerable<Dynamic>> GetUsingBooleanMultimatch(string query)
        {
            var response = await elasticClient.SearchAsync<Dynamic>(s => s
            .AllIndices()
            .Query(q => q
            .MultiMatch(m => m
            .Fields(f => f.
            Fields(new string[] {"name","age","education" })).Query(query))
            ||
            q.MultiMatch(mm => mm.
            Fields(fi => fi
            .Fields(new string[] {"title","authors","isbn"})).Query(query)
            )));

            return response?.Documents;
        }
        /// <summary>
        /// Get using wildcard operators.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<IEnumerable<Dynamic>> GetUsingWildcardMultipleFields(string query)
        {
            var res = await elasticClient.SearchAsync<Dynamic>(s => s
            .AllIndices()
            .Query(q => q
            .Wildcard(w => w
            .Field("name").Value(query+"*")) ||
            q.Wildcard(wi => wi
            .Field("title").Value(query+"*"))
            )

            );
            return res?.Documents;
        } 
        /// <summary>
        /// Gets the data from multiple fields using wildcard queries. 
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<IEnumerable<Dynamic>> GetUsingAnalyzeWildcard(string query)
        {
            var res = await elasticClient.SearchAsync<Dynamic>(s => s
            .AllIndices()
            .Query(q => q
            .QueryString(qs => qs
            .AnalyzeWildcard()
            .Query("*" + query.ToLower() + "*")
            .Fields(f => f
            .Fields("name","title", "age", "education")
            )
            )
            ));

            return res?.Documents;
        }
      
        /// <summary>
        /// Posts the newly created user details and returns its Id.
        /// </summary>
        /// <param name = "value" ></ param >
        /// < returns ></ returns >
        [HttpPost]
        public async Task<string> Post([FromBody] UserDTO value)
        {
            var response = await elasticClient.IndexAsync<UserDTO>(value, x => x.Index("users"));

            return response.Id;
        }

        /// <summary>
        /// Updates the existing record with its respective Id.
        /// </summary>
        /// <param name = "id" ></ param >
        /// < param name= "userDTO" ></ param >
        /// < returns ></ returns >
        [HttpPut]
        public async Task Put(string id, [FromBody] UserDTO userDTO)
        {

            await elasticClient.UpdateAsync(new DocumentPath<UserDTO>(id), u => u.Index("users").Doc(userDTO));

        }

        /// <summary>
        /// Deletes the existing record by passing its Id.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        [HttpDelete]
        public async Task Delete(string id)
        {

            await elasticClient.DeleteAsync(new DocumentPath<UserDTO>(id), q => q.Index("users"));
        }
    }
}
