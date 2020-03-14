using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TesteKafka
{
    /// <summary>
    /// Serviço de consumo de mensageria
    /// </summary>
    public class MensageriaWorker : IHostedService, IDisposable
    {
        private readonly ILogger<MensageriaWorker> _logger;
        private readonly IServiceProvider _services;
        private readonly IConfiguration _configuration;
        private Timer _timer;
        private IProducer<Null, string> _producer;

        //Config
        private IConfigurationSection _configurationSection => _configuration.GetSection("Mensageria");
        private int _segundosTimeoutConsumo => _configurationSection.GetValue<int>("SegundosTimeoutConsumo");
        private string _grupo => _configurationSection.GetValue<string>("Grupo");
        private string _servidoresMensageria => _configurationSection.GetValue<string>("Servidores");
        private string[] _topicos => _configurationSection.GetValue<string>("Topicos").Split(';');

        /// <summary>
        /// Inicializar worker
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="services"></param>
        /// <param name="configuration"></param>
        public MensageriaWorker(ILogger<MensageriaWorker> logger, IConfiguration configuration, IServiceProvider services)
        {
            _logger = logger;
            _services = services;
            _configuration = configuration;
        }

        /// <summary>
        /// Agenda execução
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                //if (cancellationToken.IsCancellationRequested)
                //    return Task.CompletedTask;

                StartProducer();

                //Inicializa o timer que dispara o job
                while (true)
                {
                    var intervalTime = _configurationSection.GetValue<int>("SegundosExecucaoJob");
                    //_timer = new Timer(async (state) => await TimerTick(state), cancellationToken, TimeSpan.Zero, TimeSpan.FromSeconds(intervalTime));
                    var result = await _producer.ProduceAsync(_topicos[0], new Message<Null, string> { Value = "Hello world " + DateTime.Now.ToString("HH:mm:ss") });

                    if (result == null) //Deu o timeout, volta a executar apenas no proximo tick do timer
                        return;

                    Thread.Sleep(intervalTime * 1000);
                    _producer.Flush(TimeSpan.FromSeconds(intervalTime));

                    HandleMessage(result);

                }

            }
            catch (Exception ex)
            {
                _logger.LogCritical("Erro ao inicializar o Job de Mensageria. Verifique se a configuração esta válida no appsettings!",
                                //rotina: nameof(MensageriaWorker),
                                ex);
            }


            //return Task.CompletedTask;
        }

        public void StartProducer()
        {
            var conf = new ProducerConfig
            {
                BootstrapServers = _servidoresMensageria,
                ClientId = System.Net.Dns.GetHostName(),
            };

            _producer = new ProducerBuilder<Null, string>(conf).Build();
        }

       /* public void StartConsumer()
        {
            var conf = new ConsumerConfig
            {
                GroupId = _grupo,
                BootstrapServers = _servidoresMensageria,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
            _consumer.Subscribe(_topicos);
        }*/

        /// <summary>
        /// Para execução
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            using (var scope = _services.CreateScope())
            {
                var logger = scope.ServiceProvider.GetRequiredService<ILogger<MensageriaWorker>>();

                logger.LogInformation("PARANDO: Job de Mensageria");

                _timer?.Change(Timeout.Infinite, 0);

                _producer?.Flush();

                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Realiza a execução de uma Task de tempos em tempos que é chamada pelo Timer
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        private async Task TimerTick(object state)
        {
            using (var scope = _services.CreateScope())
            {
                var logger = scope.ServiceProvider.GetRequiredService<ILogger<MensageriaWorker>>();

                try
                {
                    //logger.LogInformation("INICIANDO: Job de Mensageria");

                    while (!((CancellationToken)state).IsCancellationRequested)
                    {
                        var result = await _producer.ProduceAsync(_topicos[0], new Message<Null, string> { Value = "Hello world" });
                        Thread.Sleep(10000);
                        //_producer.Flush(TimeSpan.FromSeconds(10));

                        if (result == null) //Deu o timeout, volta a executar apenas no proximo tick do timer
                            return;

                        HandleMessage(result);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError("Erro ao executar o Job de Mensageria",
                                     //rotina: nameof(TimerTick),
                                     ex);
                }
            }
        }

        private void HandleMessage(DeliveryResult<Null, string> result)
        {
            _logger.LogInformation($"Produziu mensagem: Topico -> {result.Topic} / Msg -> {result.Message.Value}");
        }


        /*private async Task HandleMessage(ConsumeResult<Ignore, string> message)
        {
            _logger.LogInformation($"Leu mensagem: Topico -> {message.Topic} / Msg -> {message.Value.Replace("{", "").Replace("}", "")}");

            switch (message.Topic)
            {
                case "cartao-novo":
                    await HandleCartaoNovo(message.Value);
                    break;

                case "cliente-fraude-status":
                    await HandleStatusFraude(message.Value);
                    break;
            }
        }*/

        /*private async Task HandleCartaoNovo(string messageJson)
        {
            using (var scope = _services.CreateScope())
            {
                var cartaoAppService = scope.ServiceProvider.GetRequiredService<ICartaoAppService>();

                var request = JsonSerializer.Deserialize<InserirCartao.Request>(messageJson, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

                var result = await cartaoAppService.InserirCartao(request);

                if (!result.Success)
                {
                    _logger.LogError($"Erro ao processar novo cartão: {String.Join('\n', result.Errors.Select(e => e.Value))}");
                }
            }
        }*/

        /*private async Task HandleStatusFraude(string messageJson)
        {
            using (var scope = _services.CreateScope())
            {
                var clienteAppService = scope.ServiceProvider.GetRequiredService<IClienteAppService>();

                var request = JsonSerializer.Deserialize<AlterarBloqueioCliente.Request>(messageJson, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

                var result = await clienteAppService.AlterarBloqueioCliente(request);

                if (!result.Success)
                {
                    _logger.LogError($"Erro ao processar status fraude: {String.Join('\n', result.Errors.Select(e => e.Value))}");
                }
            }
        }*/

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            _timer?.Dispose();
            _producer?.Dispose();
        }
    }
}
