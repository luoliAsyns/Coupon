using Azure.Core;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using LuoliCommon.DTO.Coupon;
using LuoliCommon.Entities;
using LuoliCommon.Enums;
using LuoliCommon.Logger;
using LuoliDatabase;
using LuoliDatabase.Entities;
using LuoliDatabase.Extensions;
using MethodTimer;
using SqlSugar;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Dm.util;
using LuoliCommon.DTO.ExternalOrder;
using LuoliUtils;
using RabbitMQ.Client;
using Decoder = LuoliUtils.Decoder;
using ILogger = LuoliCommon.Logger.ILogger;

namespace CouponService
{
    // 实现服务接口
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
            _logger.Debug("starting SqlSugarCouponService.GenerateAsync ");
            var result = new ApiResponse<CouponDTO>();
            result.code= EResponseCode.Fail;
            result.data=null;

            var (valid, msg) = dto.ValidateBeforeGenCoupon();
            if (!valid)
            {
                result.msg= msg;
                return result;
            }

            try
            {
                await _sqlClient.BeginTranAsync();

                var couponDto = dto.ToCouponDTO(Program.Config.KVPairs["AppSecret2GenCoupon"]);
                couponDto.Status = ECouponStatus.Generated;
                await _sqlClient.Insertable(couponDto.ToEntity()).ExecuteCommandAsync();
                await _sqlClient.CommitTranAsync();

                result.code = EResponseCode.Success;
                result.data = couponDto;
                _logger.Debug($"SqlSugarCouponService.GenerateAsync success with dto.Tid:[{dto.Tid}] coupon:[{couponDto.Coupon}]");

                _channel.BasicPublishAsync(string.Empty, 
                   Program.Config.KVPairs["StartWith"] + RabbitMQKeys.CouponGenerated, 
                    true, 
                    _rabbitMQMsgProps,
                    Encoding.UTF8.GetBytes(JsonSerializer.Serialize(couponDto))
                    );

            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                await _sqlClient.RollbackTranAsync();
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
                CouponDTO couponDto = new CouponDTO();
                couponDto.Coupon = Decoder.SHA256(from_platform + tid);
                couponDto.ExternalOrderFromPlatform = from_platform;
                couponDto.ExternalOrderTid = tid;
                couponDto.Payment = amount;
                couponDto.AvailableBalance = couponDto.Payment;
                couponDto.CreateTime = DateTime.Now;
                couponDto.UpdateTime = couponDto.CreateTime;
                couponDto.Status = ECouponStatus.Generated;

                await _sqlClient.BeginTranAsync();
                await _sqlClient.Insertable(couponDto.ToEntity()).ExecuteCommandAsync();
                await _sqlClient.CommitTranAsync();

                result.code = EResponseCode.Success;
                result.data = couponDto;
                
                _logger.Debug(
                    $"SqlSugarCouponService.GenerateManualAsync success with from_platform:[{from_platform}] Tid:[{tid}] Payment:[{amount}] coupon:[{couponDto.Coupon}]");

                _channel.BasicPublishAsync(
                    string.Empty,
                    Program.Config.KVPairs["StartWith"] + RabbitMQKeys.CouponGenerated, 
                    true, 
                    _rabbitMQMsgProps,
                    Encoding.UTF8.GetBytes(JsonSerializer.Serialize(couponDto))
                );
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                await _sqlClient.RollbackTranAsync();
                _logger.Error(
                    $"while SqlSugarCouponService.GenerateManualAsync with from_platform:[{from_platform}] Tid:[{tid}] Payment:[{amount}]");
                _logger.Error(ex.Message);
            }

            return result;
        }


        public async Task<ApiResponse<bool>> DeleteAsync(string coupon)
        {
            _logger.Debug("starting SqlSugarCouponService.DeleteAsync ");

            var redisKey = $"coupon.{coupon}";

            var result = new ApiResponse<bool>();
            result.code = EResponseCode.Fail;
            result.data = false;

            try
            {
                await _sqlClient.BeginTranAsync();
               int impactRows= await _sqlClient.Updateable<object>()
                .AS("coupon")
                .SetColumns("is_deleted", true)
                .Where($"coupon='{coupon}'").ExecuteCommandAsync();
                await _sqlClient.CommitTranAsync();
                if (impactRows != 1)
                    throw new Exception("SqlSugarCouponService.DeleteAsync impactRows not equal to 1");

                result.code = EResponseCode.Success;
                result.data = true;
                _logger.Debug($"SqlSugarCouponService.DeleteAsync success with coupon:[{coupon}]");

                RedisHelper.DelAsync(redisKey);

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
            _logger.Debug($"starting SqlSugarCouponService.GetByTidAsync with platform:[{platform}], tid:[{tid}]");
            var result = new ApiResponse<CouponDTO>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                var redisKey = $"coupon.{platform}.{tid}";
                var couponEntity =await RedisHelper.GetAsync<CouponEntity>(redisKey);

                if (!(couponEntity is null))
                {
                    result.code = EResponseCode.Success;
                    result.data = couponEntity.ToDTO();
                    result.msg = "from redis";
                    return result;
                }

                couponEntity = await _sqlClient.Queryable<CouponEntity>()
                    .Where(o => o.external_order_tid == tid && o.external_order_from_platform == platform && o.is_deleted == 0).FirstAsync();


                result.code = EResponseCode.Success;
                result.data = couponEntity.ToDTO();
                result.msg = "from database";

                if (!(result.data is null))
                    RedisHelper.SetAsync(redisKey, couponEntity, 60);

                _logger.Debug($"SqlSugarCouponService.GetByTidAsync success with platform:[{platform}], tid:[{tid}]");

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
            _logger.Debug($"starting SqlSugarCouponService.GetAsync with coupon:[{coupon}]");
            var result = new ApiResponse<CouponDTO>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                var redisKey = $"coupon.{coupon}";
                var couponEntity = await RedisHelper.GetAsync<CouponEntity>(redisKey);

                if(!(couponEntity is null))
                {
                    result.code = EResponseCode.Success;
                    result.data = couponEntity.ToDTO();
                    result.msg = "from redis";
                    return result;
                }

                couponEntity= await _sqlClient.Queryable<CouponEntity>()
                    .Where(o=>o.coupon == coupon && o.is_deleted == 0).FirstAsync();
              
               
                result.code = EResponseCode.Success;
                result.data = couponEntity.ToDTO();
                result.msg = "from database";

                if (!(result.data is null))
                    RedisHelper.SetAsync(redisKey, couponEntity, 60);

                _logger.Debug($"SqlSugarCouponService.GetAsync success with coupon:[{coupon}]");

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
            _logger.Debug($"starting SqlSugarCouponService.GetAsync with coupons:[{string.Join(",", coupons)}]");
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
                _logger.Debug($"SqlSugarCouponService.GetAsync success with coupons:[{string.Join(",", coupons)}]");
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
            _logger.Debug("starting SqlSugarCouponService.UpdateAsync ");
            var result = new ApiResponse<bool>();
            result.code = EResponseCode.Fail;
            result.data = false;

            try
            {
                var redisKey = $"coupon.{dto.Coupon}";

                await _sqlClient.BeginTranAsync();
                int impactRows = await _sqlClient.Updateable(dto.ToEntity())
                 .Where($"external_order_from_platform='{dto.ExternalOrderFromPlatform}' and external_order_tid='{dto.ExternalOrderTid}' and is_deleted='0'").ExecuteCommandAsync();
                await _sqlClient.CommitTranAsync();
                if (impactRows != 1)
                    throw new Exception("SqlSugarCouponService.UpdateAsync impactRows not equal to 1");

                result.code = EResponseCode.Success;
                result.data = true;

                _logger.Debug($"SqlSugarCouponService.UpdateAsync success with coupon:[{dto.Coupon}]");

                RedisHelper.DelAsync(redisKey);
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
