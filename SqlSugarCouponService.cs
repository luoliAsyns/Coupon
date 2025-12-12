using Azure.Core;
using Dm.util;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using LuoliCommon.DTO.Coupon;
using LuoliCommon.DTO.ExternalOrder;
using LuoliCommon.Entities;
using LuoliCommon.Enums;
using LuoliCommon.Logger;
using LuoliDatabase;
using LuoliDatabase.Entities;
using LuoliDatabase.Extensions;
using LuoliUtils;
using MethodTimer;
using Microsoft.DotNet.PlatformAbstractions;
using RabbitMQ.Client;
using SqlSugar;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Decoder = LuoliUtils.Decoder;
using ILogger = LuoliCommon.Logger.ILogger;

namespace CouponService
{
    public class SqlSugarCouponService : ICouponService
    {
        // 注入的依赖项
        private readonly ILogger _logger;
        private readonly SqlSugarClient _sqlClient;
        private readonly IChannel _channel;
        
        private static  BasicProperties _rabbitMQMsgProps = new BasicProperties();

        // 构造函数注入
        public SqlSugarCouponService(ILogger logger, SqlSugarClient sqlClient, IChannel channel)
        {
            _logger = logger;
            _sqlClient = sqlClient;
            _channel = channel;

            _rabbitMQMsgProps.ContentType = "text/plain";
            _rabbitMQMsgProps.DeliveryMode = DeliveryModes.Persistent;
        }

        public async Task<ApiResponse<CouponDTO>> GenerateAsync(ExternalOrderDTO dto)
        {
            var result = new ApiResponse<CouponDTO>();
            result.code= EResponseCode.Fail;
            result.data=null;

            var (valid, msg) = dto.ValidateBeforeGenCoupon();
            if (!valid)
            {
                _logger.Warn($"GenerateAsync not passed ValidateBeforeGenCoupon with ExternalOrder.Tid[{dto.Tid}]");

                result.msg= msg;
                return result;
            }

            _logger.Info($"GenerateAsync passed ValidateBeforeGenCoupon with ExternalOrder.Tid[{dto.Tid}]");

            try
            {
                var couponDto = dto.ToCouponDTO(Program.Config.KVPairs["AppSecret2GenCoupon"]);
                couponDto.Status = ECouponStatus.Generated;

                _logger.Info($"GenerateAsync ExternalOrderDTO.ToCouponDTO success with ExternalOrder.Tid[{dto.Tid}]");

                await _sqlClient.Insertable(couponDto.ToEntity()).ExecuteCommandAsync();

                RedisHelper.SAddAsync(RedisKeys.NotUsedCoupons, couponDto.Coupon);
                RedisHelper.IncrByAsync(RedisKeys.Prom_CouponsGenerated);

                _logger.Info($"GenerateAsync insert into DB success with CouponDTO.Coupon[{couponDto.Coupon}]");


                result.code = EResponseCode.Success;
                result.data = couponDto;


                _channel.BasicPublishAsync(string.Empty,
                   Program.Config.KVPairs["StartWith"] + RabbitMQKeys.CouponGenerated,
                    true,
                    _rabbitMQMsgProps,
                    Encoding.UTF8.GetBytes(JsonSerializer.Serialize(couponDto))
                    );

                _logger.Info($"SqlSugarCouponService.GenerateAsync success with dto.Tid:[{dto.Tid}] coupon:[{couponDto.Coupon}], sent CouponDTO to MQ[{Program.Config.KVPairs["StartWith"] + RabbitMQKeys.CouponGenerated}]");
            
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarCouponService.GenerateAsync with dto.Tid:[{dto.Tid}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public async Task<ApiResponse<CouponDTO>> GenerateManualAsync(string from_platform, string tid, decimal amount)
        {
            var result = new ApiResponse<CouponDTO>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                _logger.Info($"GenerateManualAsync start new CouponDTO with from_platform[{from_platform}], tid[{tid}], amount[{amount}]");

                CouponDTO couponDto = new CouponDTO();
                couponDto.Coupon = Decoder.SHA256(from_platform + tid).Substring(0,32);
                couponDto.ExternalOrderFromPlatform = from_platform;
                couponDto.ExternalOrderTid = tid;
                couponDto.Payment = amount;
                couponDto.AvailableBalance = couponDto.Payment;
                couponDto.CreateTime = DateTime.Now;
                couponDto.UpdateTime = couponDto.CreateTime;
                couponDto.Status = ECouponStatus.Generated;

                _logger.Info($"GenerateManualAsync new CouponDTO success and start inserting with from_platform[{from_platform}], tid[{tid}], amount[{amount}]");

                await _sqlClient.Insertable(couponDto.ToEntity()).ExecuteCommandAsync();

                RedisHelper.SAddAsync(RedisKeys.NotUsedCoupons, couponDto.Coupon);
                RedisHelper.IncrByAsync(RedisKeys.Prom_CouponsGenerated);

                _logger.Info($"GenerateManualAsync insert into DB success with CouponDTO.Coupon[{couponDto.Coupon}]");


                result.code = EResponseCode.Success;
                result.data = couponDto;
                
                _channel.BasicPublishAsync(
                    string.Empty,
                    Program.Config.KVPairs["StartWith"] + RabbitMQKeys.CouponGenerated, 
                    true, 
                    _rabbitMQMsgProps,
                    Encoding.UTF8.GetBytes(JsonSerializer.Serialize(couponDto))
                );

                _logger.Info($"SqlSugarCouponService.GenerateManualAsync success with coupon:[{couponDto.Coupon}], sent CouponDTO to MQ[{Program.Config.KVPairs["StartWith"] + RabbitMQKeys.CouponGenerated}]");

            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarCouponService.GenerateManualAsync with from_platform:[{from_platform}] Tid:[{tid}] Payment:[{amount}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public async Task<ApiResponse<bool>> DeleteAsync(string coupon)
        {
            var redisKey = $"coupon.{coupon}";

            var result = new ApiResponse<bool>();
            result.code = EResponseCode.Fail;
            result.data = false;

            try
            {
                _logger.Info($"DeleteAsync BeginTran with coupon:[{coupon}]");

                await _sqlClient.BeginTranAsync();

                int impactRows= await _sqlClient.Updateable<object>()
                    .AS("coupon")
                    .SetColumns("is_deleted", true)
                    .Where($"coupon='{coupon}'")
                    .ExecuteCommandAsync();

                if (impactRows != 1)
                    throw new Exception("SqlSugarCouponService.DeleteAsync impactRows not equal to 1");

                await _sqlClient.CommitTranAsync();

                _logger.Info($"DeleteAsync commit success with coupon:[{coupon}]");

                result.code = EResponseCode.Success;
                result.data = true;

                RedisHelper.DelAsync(redisKey);

                _logger.Info($"SqlSugarCouponService.DeleteAsync success with coupon:[{coupon}], remove cache");

            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                await _sqlClient.RollbackTranAsync();
                _logger.Error($"while SqlSugarCouponService.DeleteAsync with coupon:[{coupon}]");
                _logger.Error(ex.Message);
            }

            return result;
        }
       
        public async Task<ApiResponse<CouponDTO>> GetByTidAsync(string platform,string tid)
        {
            var result = new ApiResponse<CouponDTO>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                var redisKey = $"coupon.{platform}.{tid}";
                var couponEntity =await RedisHelper.GetAsync<CouponEntity>(redisKey);

                if (!(couponEntity is null))
                {
                    _logger.Info($"cache hit for key:[{redisKey}]");
                    result.code = EResponseCode.Success;
                    result.data = couponEntity.ToDTO();
                    result.msg = "from redis";
                    return result;
                }

                _logger.Info($"cache miss for key:[{redisKey}]");

                couponEntity = await _sqlClient.Queryable<CouponEntity>()
                    .Where(o => o.external_order_tid == tid && o.external_order_from_platform == platform && o.is_deleted == 0).FirstAsync();

                result.code = EResponseCode.Success;
                result.data = couponEntity.ToDTO();
                result.msg = "from database";

                if (result.data is null)
                    _logger.Warn($"SqlSugarCouponService.GetByTidAsync success with platform:[{platform}], tid:[{tid}], but data is null");
                else
                {
                    RedisHelper.SetAsync(redisKey, couponEntity, 60);
                    _logger.Info($"SqlSugarCouponService.GetByTidAsync success with platform:[{platform}], tid:[{tid}], add it into cache");
                }


            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarCouponService.GetByTidAsync with platform:[{platform}], tid:[{tid}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public async Task<ApiResponse<CouponDTO>> GetAsync(string coupon)
        {
            var result = new ApiResponse<CouponDTO>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                var redisKey = $"coupon.{coupon}";
                var couponEntity = await RedisHelper.GetAsync<CouponEntity>(redisKey);

                if(!(couponEntity is null))
                {
                    _logger.Info($"cache hit for key:[{redisKey}]");
                    result.code = EResponseCode.Success;
                    result.data = couponEntity.ToDTO();
                    result.msg = "from redis";
                    return result;
                }

                _logger.Info($"cache miss for key:[{redisKey}]");

                couponEntity = await _sqlClient.Queryable<CouponEntity>()
                    .Where(o=>o.coupon == coupon && o.is_deleted == 0).FirstAsync();
              
               
                result.code = EResponseCode.Success;
                result.data = couponEntity.ToDTO();
                result.msg = "from database";


                if (result.data is null)
                    _logger.Warn($"SqlSugarCouponService.GetAsync success with coupon:[{coupon}], but data is null");
                else
                {
                    RedisHelper.SetAsync(redisKey, couponEntity, 60);
                    _logger.Info($"SqlSugarCouponService.GetAsync success with coupon:[{coupon}], add it into cache");
                }

            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarCouponService.GetAsync with  with coupon:[{coupon}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public async Task<ApiResponse<List<CouponDTO>>> GetAsync(string[] coupons, ECouponStatus? status= null)
        {

            var result = new ApiResponse<List<CouponDTO>>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                if (coupons is null || coupons.Length == 0)
                {
                    result.code = EResponseCode.Success;
                    result.msg = "input coupons is blank";
                    result.data  =  Array.Empty<CouponDTO>().ToList();
                    _logger.Warn("while SqlSugarCouponService.GetAsync(string[] coupons, ECouponStatus? status= null), input coupons is blank");
                    return result;
                }

                List<Task<CouponDTO>> tasks = coupons.Distinct().Select(async coupon =>
                {
                    var result = await GetAsync(coupon);
                    return result.data;
                }).ToList();

                // 等待所有任务完成
                await Task.WhenAll(tasks);
                // 收集结果（结果顺序与原coupons数组一致）

                var query = tasks.Select(t => t.Result).Where(dto => !(dto is null));
                if (status.HasValue)
                    query = query.Where(dto=>dto.Status == status.Value);

                
                result.data = query.ToList();
                result.code = EResponseCode.Success;
                _logger.Info($"SqlSugarCouponService.GetAsync success with coupons:[{string.Join(",", coupons)}], return coupons:[{string.Join(",", result.data.Select(co=>co.Coupon))}]");
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarCouponService.GetAsync with coupons:[{string.Join(",", coupons)}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public  async Task<ApiResponse<PageResult<CouponDTO>>> GetAsync(int page,
            int size,
            byte? status = null,
            DateTime? startTime = null,
            DateTime? endTime = null)
        {
            
            ApiResponse<PageResult<CouponDTO>> response = new ();

            response.code = EResponseCode.Fail;
            response.data = null;
            
            try
            {
                var query = _sqlClient
                    .Queryable<CouponEntity>()
                    .Where(o => o.is_deleted == 0); // 排除已删除的订单

                // 动态添加筛选条件
                if (status.HasValue)
                    query = query.Where(o => o.status == status.Value);

                if (startTime.HasValue)
                    query = query.Where(o => o.create_time >= startTime.Value);

                if (endTime.HasValue)
                    query = query.Where(o => o.create_time <= endTime.Value);

                // 4. 执行分页查询（先查总数，再查当前页数据）
                long total = await query.CountAsync();
                List<CouponEntity> coupons = await query
                    .OrderByDescending(o => o.create_time) // 按创建时间倒序
                    .Skip((page-1)* size)
                    .Take(size)
                    .ToListAsync();

                // 4. 构建分页结果
                var pageResult = new PageResult<CouponDTO>
                {
                    Total = total,       // 总记录数
                    Page = page,         // 当前页码
                    Size = size,         // 每页大小
                    Items = coupons.Select(entity=>entity.ToDTO()).ToList()      // 当前页数据列表
                };

                // 5. 返回统一响应格式
                response.data = pageResult;
                response.msg = "success";
                response.code = EResponseCode.Success;
            }
            catch (Exception ex)
            {
                response.msg = ex.Message;
                _logger.Error($"while SqlSugarCouponService.GetAsync with page:[{page}] size:[{size}]");
                _logger.Error(ex.Message);
            }
            return response;
        }

        public async Task<ApiResponse<bool>> UpdateAsync(CouponDTO dto)
        {
            var result = new ApiResponse<bool>();
            result.code = EResponseCode.Fail;
            result.data = false;
            var redisKeys = new string[]{ $"coupon.{dto.Coupon}", $"coupon.{dto.ExternalOrderFromPlatform}.{dto.ExternalOrderTid}"};

            try
            {
                _logger.Info($"UpdateAsync BeginTran with CouponDTO.Coupont:[{dto.Coupon}]");

                await _sqlClient.BeginTranAsync();

                int impactRows = await _sqlClient.Updateable(dto.ToEntity())
                     .Where($"external_order_from_platform='{dto.ExternalOrderFromPlatform}' and external_order_tid='{dto.ExternalOrderTid}' and is_deleted='0'")
                     .IgnoreColumns(it => new { it.coupon, it.external_order_from_platform, it.external_order_tid })
                     .ExecuteCommandAsync();

                if (impactRows != 1)
                    throw new Exception("SqlSugarCouponService.UpdateAsync impactRows not equal to 1");

                await _sqlClient.CommitTranAsync();

                _logger.Info($"UpdateAsync commit success with CouponDTO.Coupont:[{dto.Coupon}]");

                result.code = EResponseCode.Success;
                result.data = true;

                foreach(var key in redisKeys)
                    await RedisHelper.DelAsync(key);

                _logger.Info($"SqlSugarCouponService.UpdateAsync success with coupon:[{dto.Coupon}]");
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                await _sqlClient.RollbackTranAsync();
                _logger.Error($"while SqlSugarCouponService.UpdateAsync with coupon:[{dto.Coupon}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

    }
}
