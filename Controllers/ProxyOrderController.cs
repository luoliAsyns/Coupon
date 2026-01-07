using LuoliCommon.DTO.ProxyOrder;
using LuoliCommon.Enums;
using LuoliCommon.Interfaces;
using MethodTimer;
using Microsoft.AspNetCore.Mvc;
using Refit;
using static Azure.Core.HttpHeader;
using ILogger = LuoliCommon.Logger.ILogger;

namespace CouponService.Controllers
{
    public class ProxyOrderController : Controller, IProxyOrderService
    {

        private readonly ILogger _logger;
        private readonly IProxyOrderRepo _proxyOrderRepo;

        public ProxyOrderController(ILogger logger, IProxyOrderRepo  proxyOrderRepo)
        {
            _logger = logger;
            _proxyOrderRepo = proxyOrderRepo;
        }


        [Time]
        [HttpGet]
        [Route("api/proxy-order/query")]
        public async Task<LuoliCommon.Entities.ApiResponse<ProxyOrderDTO>> GetAsync(string coupon)
        {
            return await _proxyOrderRepo.GetAsync(coupon);
        }


        [Time]
        [HttpGet]
        [Route("api/proxy-order/query-coupons")]
        public async Task<LuoliCommon.Entities.ApiResponse<IEnumerable<ProxyOrderDTO>>> GetAsync(string targetProxy, string[] coupons, string? orderStatus = null)
        {
            return await _proxyOrderRepo.GetAsync(targetProxy, coupons, orderStatus);
        }


        [Time]
        [HttpPost]
        [Route("api/proxy-order/insert")]
        public async Task<LuoliCommon.Entities.ApiResponse<bool>> InsertAsync([FromBody] ProxyOrderDTO dto)
        {
            return await _proxyOrderRepo.InsertAsync(dto);
        }

        [Time]
        [HttpPost]
        [Route("api/proxy-order/update")]
        public async Task<LuoliCommon.Entities.ApiResponse<bool>> UpdateAsync([FromBody] ProxyOrderDTO dto)
        {
            return await _proxyOrderRepo.UpdateAsync(dto);
        }
    }
}
