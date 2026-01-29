using Azure;
using Grpc.Core;
using LuoliCommon.DTO.ConsumeInfo.Sexytea;
using LuoliCommon.DTO.Coupon;
using LuoliCommon.DTO.ExternalOrder;
using LuoliCommon.DTO.ProxyOrder;
using LuoliCommon.Entities;
using LuoliCommon.Enums;
using LuoliCommon.Interfaces;
using LuoliDatabase;
using LuoliDatabase.Entities;
using LuoliDatabase.Extensions;
using LuoliUtils;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.DotNet.PlatformAbstractions;
using RabbitMQ.Client;
using SqlSugar;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ThirdApis;
using static Azure.Core.HttpHeader;
using ILogger = LuoliCommon.Logger.ILogger;

namespace CouponService
{
    public class SqlSugarProxyOrderRepo : IProxyOrderRepo
    {
        // 注入的依赖项
        private readonly ILogger _logger;
        private readonly SqlSugarClient _sqlClient;
        private readonly ICouponRepo _couponRepo;
        private readonly IExternalOrderService _eoService;


        private readonly SexyteaApis _sexyteaApis ;

        // 构造函数注入
        public SqlSugarProxyOrderRepo(
            ILogger logger, 
            SqlSugarClient sqlClient,
            ICouponRepo couponRepo, 
            IExternalOrderService eoService, 
            SexyteaApis sexyteaApis)
        {
            _logger = logger;
            _sqlClient = sqlClient;
            _couponRepo = couponRepo;
            _eoService = eoService;
            _sexyteaApis = sexyteaApis;
        }

        public async Task<ApiResponse<bool>> DeleteAsync(string targetProxy, string proxyOrderId)
        {
            var redisKey = $"proxyorder.{targetProxy}.{proxyOrderId}";

            var result = new ApiResponse<bool>();
            result.code = EResponseCode.Fail;
            result.data = false;

            try
            {
                _logger.Info($"SqlSugarProxyOrderRepo.DeleteAsync BeginTran with target_proxy:[{targetProxy}], proxy_order_id:[{proxyOrderId}]");

                await _sqlClient.BeginTranAsync();

                int impactRows = await _sqlClient.Updateable<object>()
                    .AS($"{targetProxy}_order")
                    .SetColumns("is_deleted", true)
                    .Where($"target_proxy='{targetProxy}' and proxy_order_id='{proxyOrderId}'")
                    .ExecuteCommandAsync();

                if (impactRows != 1)
                    throw new Exception("SqlSugarProxyOrderRepo.DeleteAsync impactRows not equal to 1");

                await _sqlClient.CommitTranAsync();

                _logger.Info($"SqlSugarProxyOrderRepo.DeleteAsync commit success with target_proxy:[{targetProxy}], proxy_order_id:[{proxyOrderId}]");

                result.code = EResponseCode.Success;
                result.data = true;

                RedisHelper.DelAsync(redisKey);

                _logger.Info($"SqlSugarProxyOrderRepo.DeleteAsync success with target_proxy:[{targetProxy}], proxy_order_id:[{proxyOrderId}], remove cache");

            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                await _sqlClient.RollbackTranAsync();
                _logger.Error($"while SqlSugarProxyOrderRepo.DeleteAsync with target_proxy:[{targetProxy}], proxy_order_id:[{proxyOrderId}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public async Task<ApiResponse<ProxyOrderDTO>> GetAsync(string coupon)
        {
            var result = new ApiResponse<ProxyOrderDTO>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                // 通过 coupon 获取 CouponDTO  -- ProxyOpenId  ProxyOrderId
                var couponDTO = (await _couponRepo.GetAsync(coupon)).data;
                if (couponDTO is null)
                {
                    result.msg = $"while SqlSugarProxyOrderRepo.GetAsync, cannot find coupon:[{coupon}]";
                    _logger.Warn(result.msg);
                    return result;
                }

                // 通过 CouponDTO 获取 ExternalOrderDTO  -- TargetProxy
                var eoDTO = (await _eoService.Get(couponDTO.ExternalOrderFromPlatform, couponDTO.ExternalOrderTid)).data;
                if (eoDTO is null)
                {
                    result.msg = $"while SqlSugarProxyOrderRepo.GetAsync, cannot find external order:[{couponDTO.ExternalOrderFromPlatform},{couponDTO.ExternalOrderTid}]";
                    _logger.Warn(result.msg);
                    return result;
                }
                //var eoDTO = (await RedisHelper.GetAsync<ExternalOrderEntity>("externalorder.TAOBAO.4993700331617317514")).ToDTO();


                // 超过1天了
                if ((DateTime.Now- couponDTO.CreateTime ).TotalDays > 1)
                {
                    await GetFromLocalAsync(result, couponDTO, eoDTO);

                    // 本地找到了
                    if (result.code ==  EResponseCode.Success)
                        return result;
                }

                // 本地没找到 或者是最近1天的订单，直接从第三方Api获取最新状态
                if (eoDTO.TargetProxy == ETargetProxy.sexytea)
                {
                    var account = await RedisHelper.HGetAsync<Account>(RedisKeys.SexyteaTokenAccount, couponDTO.ProxyOpenId);

                    if (account is null || account.Exp < DateTime.Now)
                    {
                        result.code = EResponseCode.Fail;
                        result.msg = $"Sexytea token[{couponDTO.ProxyOpenId}] expired";
                        result.data = null;
                        _logger.Warn(result.msg);
                        return result;
                    }

                    var proxyOrderDTO = new ProxyOrderDTO(eoDTO, couponDTO);
                    var order = await _sexyteaApis.GetOrderInfo(account, couponDTO.ProxyOrderId);
                    if (order is null)
                    {
                        result.code = EResponseCode.Fail;
                        result.msg = $"SexyteaApi found nothing with ProxyOrderId:[{couponDTO.ProxyOrderId}]";
                        result.data = null;
                        _logger.Warn(result.msg);
                        return result;
                    }


                    proxyOrderDTO.Order = order.ToString();

                    using JsonDocument doc = JsonDocument.Parse(proxyOrderDTO.Order);
                    proxyOrderDTO.OrderStatus = doc.RootElement
                        .GetProperty("data")
                        .GetProperty("status").GetString();

                    result.data = proxyOrderDTO;

                }
                else
                {
                    result.code = EResponseCode.Fail;
                    result.data = null;
                    result.msg = $"while SqlSugarProxyOrderRepo.GetAsync, unknown eoDTO.TargetProxy:[{eoDTO.TargetProxy.ToString()}]";
                    _logger.Warn(result.msg);
                    return result;
                }    

                result.code = EResponseCode.Success;
                result.msg = "from api";
                
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarProxyOrderRepo.GetAsync with coupon:[{coupon}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        private async Task GetFromLocalAsync(ApiResponse<ProxyOrderDTO> result, CouponDTO couponDTO, ExternalOrderDTO eoDTO)
        {
            // 先从缓存获取
            var redisKey = $"proxyorder.{couponDTO.Coupon}";

            var proxyOrderEntity = await RedisHelper.GetAsync<ProxyOrderEntity>(redisKey);

            if (!(proxyOrderEntity is null))
            {
                _logger.Info($"cache hit for key:[{redisKey}]");
                result.code = EResponseCode.Success;
                result.data = proxyOrderEntity.ToDTO();
                result.msg = "from redis";
                return;
            }

            _logger.Info($"cache miss for key:[{redisKey}]");

            // 缓存未命中，从数据库获取
            string strTargetProxy = eoDTO.TargetProxy.ToString();

            proxyOrderEntity = await _sqlClient.Queryable<ProxyOrderEntity>()
                .AS($"{strTargetProxy}_order")
                .Where(o => o.target_proxy == strTargetProxy && o.proxy_order_id == couponDTO.ProxyOrderId && o.is_deleted == 0)
                .FirstAsync();


            if (proxyOrderEntity is null)
            {
                _logger.Info($"database miss for key:[{redisKey}]");
                return;
            }

            _logger.Info($"database hit for key:[{redisKey}]");
            result.code = EResponseCode.Success;
            result.data = proxyOrderEntity.ToDTO();
            result.msg = "from database";

            // 数据库里查到了, 写入缓存
            int cacheStoreSec = await RedisHelper.GetAsync<int>(RedisKeys.ProxyOrderCacheStoreSec);
            if (cacheStoreSec <= 0)
                cacheStoreSec = 60;

            RedisHelper.SetAsync(redisKey, proxyOrderEntity, cacheStoreSec);

            _logger.Info($"SqlSugarProxyOrderRepo.GetAsync success coupon:[{couponDTO.Coupon}], add it into cache");

        }

        public async Task<ApiResponse<IEnumerable<ProxyOrderDTO>>> GetAsync(string targetProxy, string[] coupons, string? orderStatus = null)
        {
            var result = new ApiResponse<IEnumerable<ProxyOrderDTO>>();
            result.code = EResponseCode.Fail;
            result.data = null;

            try
            {
                if (coupons is null || coupons.Length == 0)
                {
                    result.code = EResponseCode.Success;
                    result.msg = "input coupons is blank";
                    result.data = Array.Empty<ProxyOrderDTO>();
                    _logger.Warn("while SqlSugarProxyOrderRepo.GetAsync(string targetProxy, string[] coupons, string? orderStatus = null), input coupons is blank");
                    return result;
                }

                List<Task<ProxyOrderDTO>> tasks = coupons.Distinct().Select(async coupon =>
                {
                    var result = await GetAsync(coupon);
                    return result.data;
                }).ToList();

                // 等待所有任务完成
                await Task.WhenAll(tasks);
                // 收集结果（结果顺序与原coupons数组一致）

                var query = tasks.Select(t => t.Result)
                    .Where(dto => !(dto is null))
                    .Where(dto => dto.TargetProxy.ToString() == targetProxy);

                if (!(orderStatus is null))
                    query = query.Where(dto => dto.OrderStatus == orderStatus);


                result.data = query.ToList();
                result.code = EResponseCode.Success;
                _logger.Info($"SqlSugarProxyOrderRepo.GetAsync success with targetProxy:[{targetProxy}], coupons:[{string.Join(",", coupons)}], return proxyOrders:[{string.Join(",", result.data.Select(co => co.ProxyOrderId))}]");
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarProxyOrderRepo.GetAsync with targetProxy:[{targetProxy}], coupons:[{string.Join(",", coupons)}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public async Task<ApiResponse<bool>> InsertAsync(ProxyOrderDTO dto)
        {
            var result = new ApiResponse<bool>();
            result.code = EResponseCode.Fail;
            result.data = false;

            _logger.Info($"SqlSugarProxyOrderRepo.InserAsync with ProxyOrderDTO.ProxyOrderId[{dto.ProxyOrderId}]");

            try
            {
                var entity = dto.ToEntity();
                if(entity is null)
                {
                    result.msg = "SqlSugarProxyOrderRepo.InserAsync, entity is null";
                    _logger.Warn(result.msg);
                    return result;
                }

                int impactRows = await _sqlClient.Insertable(entity)
                  .AS($"{dto.TargetProxy.ToString()}_order")
                  .ExecuteCommandAsync();

                if (impactRows != 1)
                    throw new Exception("SqlSugarProxyOrderRepo.InsertAsync impactRows not equal to 1");

                _logger.Info($"SqlSugarProxyOrderRepo.InserAsync success with ProxyOrderDTO.ProxyOrderId[{dto.ProxyOrderId}]");


                result.code = EResponseCode.Success;
                result.data = true;

            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarProxyOrderRepo.InserAsync with ProxyOrderDTO.ProxyOrderId[{dto.ProxyOrderId}]");
                _logger.Error(ex.Message);
            }

            return result;
        }

        public async Task<ApiResponse<bool>> UpdateAsync(ProxyOrderDTO dto)
        {
            var result = new ApiResponse<bool>();
            result.code = EResponseCode.Fail;
            result.data = false;

            var strTargetProxy = dto.TargetProxy.ToString();
            var redisKey = $"proxyorder.{strTargetProxy}.{dto.ProxyOrderId}";

            try
            {
                _logger.Info($"SqlSugarProxyOrderRepo.UpdateAsync BeginTran with TargetProxy:[{strTargetProxy}], ProxyOrderId:[{dto.ProxyOrderId}]");

                await _sqlClient.BeginTranAsync();

                int impactRows = await _sqlClient.Updateable(dto.ToEntity())
                     .Where($"target_proxy='{strTargetProxy}' and proxy_order_id='{dto.ProxyOrderId}' and is_deleted='0'")
                     .IgnoreColumns(it => new {it.id, it.proxy_order_id, it.target_proxy })
                     .ExecuteCommandAsync();

                if (impactRows != 1)
                    throw new Exception("SqlSugarProxyOrderRepo.UpdateAsync impactRows not equal to 1");

                await _sqlClient.CommitTranAsync();

                _logger.Info($"SqlSugarProxyOrderRepo.UpdateAsync commit success with TargetProxy:[{strTargetProxy}], ProxyOrderId:[{dto.ProxyOrderId}]");

                result.code = EResponseCode.Success;
                result.data = true;

                await RedisHelper.DelAsync(redisKey);

                _logger.Info($"SqlSugarProxyOrderRepo.UpdateAsync success with TargetProxy:[{strTargetProxy}], ProxyOrderId:[{dto.ProxyOrderId}]");
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                await _sqlClient.RollbackTranAsync();
                _logger.Error($"while SqlSugarProxyOrderRepo.UpdateAsync with TargetProxy:[{strTargetProxy}], ProxyOrderId:[{dto.ProxyOrderId}]");
                _logger.Error(ex.Message);
            }

            return result;
        }
    
        public async Task<ApiResponse<int>> BackUpAsync(BackUpRequest request)
        {
            var result = new ApiResponse<int>();
            result.code = EResponseCode.Fail;
            result.data = 0;
            try
            {
                string strTargetProxy = request.TargetProxy.ToString();

                List<string> coupons = await _sqlClient.Queryable<CouponEntity, ExternalOrderEntity>((co, eo) =>
                        // 核心：指定 INNER JOIN 的关联条件（On 部分）
                        co.external_order_from_platform == eo.from_platform
                        && co.external_order_tid == eo.tid)
                        
                    .Where((co, eo) => co.create_time > request.From && co.create_time < request.To // 时间范围
                                    && eo.target_proxy == strTargetProxy                            // 指定品牌
                                    && co.proxy_order_id.Length > 3)                                // 有代理订单号
                    .Select((co, eo) => co.coupon)
                    .ToListAsync();

                _logger.Info($"SqlSugarProxyOrderRepo.BackUpAsync found {coupons.Count} coupons between [{request.From}] and [{request.To}] in [{strTargetProxy}]");

                int insertedCount = 0;
                foreach (var coupon in coupons)
                { 
                    var proxyOrderResp = await GetAsync(coupon);
                    if(proxyOrderResp.ok && proxyOrderResp.msg == "from api")
                    {
                        await InsertAsync(proxyOrderResp.data);
                        insertedCount++;
                    }
                }

                _logger.Info($"SqlSugarProxyOrderRepo.BackUpAsync inserted {insertedCount} ProxyOrder into database");

                result.code = EResponseCode.Success;
                result.data = insertedCount;
            }
            catch (Exception ex)
            {
                result.msg = ex.Message;
                _logger.Error($"while SqlSugarProxyOrderRepo.BackUpAsync with TargetProxy:[{request.TargetProxy.ToString()}] between [{request.From}] and [{request.To}]");
                _logger.Error(ex.Message);
            }
            return result;
        }
    }
}
