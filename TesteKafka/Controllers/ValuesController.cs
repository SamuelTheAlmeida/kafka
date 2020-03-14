using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace TesteKafka.Controllers
{
    [Route("api/")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        // GET api/
        [HttpGet]
        public async Task<List<string>> Get()
        {
             var retorno = new List<string>();

             var consumerConfig = new ConsumerConfig
             {
                 BootstrapServers = "172.16.4.55:9092",
                 GroupId = "testes-kafka",
                 AutoOffsetReset = AutoOffsetReset.Earliest,
                 EnableAutoCommit = true
             };

             using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
             {
                 consumer.Subscribe("teste-kafka");

                bool cancelled = false;
                while (!cancelled)
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(8));
                    if (consumeResult == null)
                        cancelled = true;
                    else
                        retorno.Add($"Consumido: {consumeResult.Message.Value}");
                }

                consumer.Close();
            }

             return retorno;
        }


    }
}
