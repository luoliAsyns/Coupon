using CouponService.Controllers;
using LuoliCommon.DTO.ExternalOrder;
using LuoliUtils;
using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using ThirdApis;

namespace CouponService
{
    public class ConsumerService : BackgroundService
    {
        private readonly IChannel _channel;
        private readonly IServiceProvider _serviceProvider;
        private readonly string _queueName = Program.Config.KVPairs["StartWith"] + RabbitMQKeys.ExternalOrderInserted; // 替换为你的队列名
        private readonly LuoliCommon.Logger.ILogger _logger;
        public ConsumerService(IChannel channel,
             IServiceProvider serviceProvider,
             LuoliCommon.Logger.ILogger logger
             )
        {
            _channel = channel;
            _logger = logger;
            _serviceProvider = serviceProvider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 声明队列
            await _channel.QueueDeclareAsync(
                queue: _queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: stoppingToken);

            // 设置Qos
            await _channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 10,
                global: false,
                stoppingToken);

            // 创建消费者
            var consumer = new AsyncEventingBasicConsumer(_channel);

            // 处理接收到的消息
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                try
                {
                    _logger.Info("Coupon.ConsumerService[For Generate Coupon] received message");
                    _logger.Debug(message);

                    var dto = JsonSerializer.Deserialize<ExternalOrderDTO>(message);
                    // 使用ServiceProvider创建作用域，以便获取Controller实例
                    using (var scope = _serviceProvider.CreateScope())
                    {
                        // 获取你的Controller实例
                        ICouponService cs = scope.ServiceProvider.GetRequiredService<ICouponService>();
                        AsynsApis asynsApis = scope.ServiceProvider.GetRequiredService<AsynsApis>();

                        //这里要重新获取DTO，因为有可能改变了
                        var resp = await asynsApis.ExternalOrderQuery(dto.FromPlatform, dto.Tid);
                        if (!resp.ok)
                        {
                            _logger.Error($"Coupon.ConsumerService query ExternalOrderDTO failed with FromPlatform[{dto.FromPlatform}], Tid[{dto.Tid}] so nack it");
                            // 查询失败
                            await _channel.BasicNackAsync(
                               deliveryTag: ea.DeliveryTag,
                               multiple: false,
                               requeue: false,
                               stoppingToken);
                            return;
                        }

                        dto = resp.data;

                        // 调用Controller中的方法
                        var result = await cs.GenerateAsync(dto);

                        // 如果需要处理返回结果
                        if (result.ok)
                        {
                            _logger.Info("Coupon.ConsumerService Generate Coupon success, so ack it");

                            // 处理成功，确认消息
                            await _channel.BasicAckAsync(
                                deliveryTag: ea.DeliveryTag,
                                multiple: false,
                                stoppingToken);
                        }
                        else
                        {
                            // 处理失败
                            _logger.Error("while ConsumerService call GenerateAsync, Generate Coupon failed, so nack it");
                            await _channel.BasicNackAsync(
                                deliveryTag: ea.DeliveryTag,
                                multiple: false,
                                requeue: false,
                                stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("while ConsumerService consuming");
                    _logger.Error(ex.Message);
                    // 处理异常，记录日志
                    // 异常情况下不确认消息，不重新入队
                    await _channel.BasicNackAsync(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false,
                        requeue: false,
                        stoppingToken);
                }
            };

            _logger.Info($"Coupon.ConsumerService start listen MQ[{_queueName}]");


            // 开始消费
            await _channel.BasicConsumeAsync(
                queue: _queueName,
                autoAck: false,
                consumerTag: Program.Config.ServiceName,
                noLocal: false,
                exclusive: false,
                arguments: null,
                consumer: consumer,
                stoppingToken);

            // 保持服务运行直到应用程序停止
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
    }



}
